from dagster import (
    multi_asset,
    Field,
    EnvVar,
    AssetOut,
)

import os
import pickle
import glob
from pathlib import Path
from datetime import datetime


def _standardise_file_extention(file) -> None:
    """Change file extension when '.gbk' for '.gb'"""
    _path = Path(file)
    if _path.suffix == ".gbk":
        return _path.rename(_path.with_suffix(".gb"))
    else:
        return _path


# ______ Used for the sensor

# class FileConfig(Config):
#     filename: str


# @op(
#     retry_policy=RetryPolicy(
#         max_retries=3,
#         delay=0.2,  # 200ms
#     )
# )
# def process_file(context, config: FileConfig):
#     """Print file name on console"""
#     context.log.info(config.filename)
#     return config.filename


# @graph_asset()
# def process_asset():
#     return process_file()

# ______________


sqc_folder_config = {
    "fs": Field(
        str,
        description="Path to folder containing the genbank files",
        default_value="fs",
    ),
    "genbank_dir": Field(
        str,
        description="Path to folder containing the genbank files",
        default_value="genbank",
    ),
    "fasta_dir": Field(
        str,
        description="Path to folder containing the fasta sequence files",
        default_value="gene_identity/fasta",
    ),
}


@multi_asset(
    config_schema={**sqc_folder_config},
    outs={
        "standardised_ext_file": AssetOut(
            is_required=True,
            description="Return the newly processed file relative path with standardised extention",
            io_manager_key="io_manager",
            metadata={
                "owner": "Virginie Grosboillot",
            },
        ),
        "list_genbank_files": AssetOut(
            is_required=True,
            description="Update the list of sequences available in the genbank folder",
            io_manager_key="io_manager",
            metadata={
                "owner": "Virginie Grosboillot",
            },
        ),
    },
    compute_kind="Python",
)
def list_genbank_files(context):  # -, process_asset - > List[str]:
    # List files in the genbank directory
    _gb_path = "/".join([os.getenv("PHAGY_DIRECTORY"), context.op_config["genbank_dir"]])

    # filepath = "/".join(
    #     [os.getenv("PHAGY_DIRECTORY"), context.op_config["genbank_dir"], process_asset]
    # )

    # Load already processed files
    _path = "/".join(
        [
            os.getenv(EnvVar("PHAGY_DIRECTORY")),
            context.op_config["fs"],
            "list_genbank_files",
        ]
    )

    if os.path.exists(_path):
        context.log.info("path exist")
        _files = pickle.load(open(_path, "rb"))
    else:
        context.log.info("path do not exist")
        _files = []
    context.log.info(_files)

    _new_files = []
    _new_paths = []
    for _file in glob.glob(f'{_gb_path}/*.gb*'):  #os.listdir(gb_path):
        if Path(_file).stem not in _files:
            context.log.info(f"The following file {_file} is being processed")
            #filepath = "/".join([gb_path, file])
            # Standarise file extension
            _new_path = _standardise_file_extention(_file)
            # new_path = _standardise_file_extention(process_asset)
            _new_paths.append(_new_path)
            _new_files.append(_new_path.stem)

    # Update file list
    for _new_file in _new_files:
        _files.append(_new_file)

    # Asset metadata
    _time = datetime.now()
    context.add_output_metadata(
        output_name="standardised_ext_file",
        metadata={
            "text_metadata": f"Last update of the genbank list {_time.isoformat()} (UTC).",
            "processed_files": _new_files,
            "num_files": len(_files),
            "path": _path,
        },
    )
    context.add_output_metadata(
        output_name="list_genbank_files",
        metadata={
            "text_metadata": f"Last update of the genbank list {_time.isoformat()} (UTC).",
            "path": _path,
            "num_files": len(_files),
            "updated_list": _files,
        },
    )
    return _new_paths, _files
