from dagster import (
    multi_asset,
    op,
    graph_asset,
    Config,
    Field,
    EnvVar,
    AssetOut,
    RetryPolicy,
)

import os
import pickle
from pathlib import Path
from datetime import datetime


def _standardise_file_extention(file) -> None:
    """Change file extension when '.gbk' for '.gb'"""
    path = Path(file)
    if path.suffix == ".gbk":
        return path.rename(path.with_suffix(".gb"))
    else:
        return path


class FileConfig(Config):
    filename: str


@op(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=0.2,  # 200ms
    )
)
def process_file(context, config: FileConfig):
    """Print file name on console"""
    context.log.info(config.filename)
    return config.filename


@graph_asset()
def process_asset():
    return process_file()


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
def list_genbank_files(context, process_asset):  # -> List[str]:
    context.log.info(f"The following file {process_asset} is being processed")

    filepath = "/".join(
        [os.getenv("PHAGY_DIRECTORY"), context.op_config["genbank_dir"], process_asset]
    )

    # Standarise file extension
    new_path = _standardise_file_extention(filepath)
    # new_path = _standardise_file_extention(process_asset)
    new_file = new_path.stem

    # Load already processed files
    path = "/".join(
        [
            os.getenv(EnvVar("PHAGY_DIRECTORY")),
            context.op_config["fs"],
            "list_genbank_files",
        ]
    )

    if os.path.exists(path):
        context.log.info("path exist")
        files = pickle.load(open(path, "rb"))
    else:
        context.log.info("path do not exist")
        files = []
    context.log.info(files)

    # Update file list
    files.append(new_file)

    # Asset metadata
    time = datetime.now()
    context.add_output_metadata(
        output_name="standardised_ext_file",
        metadata={
            "text_metadata": f"Last update of the genbank list {time.isoformat()} (UTC).",
            "processed_file": new_file,
            "path": path,
        },
    )
    context.add_output_metadata(
        output_name="list_genbank_files",
        metadata={
            "text_metadata": f"Last update of the genbank list {time.isoformat()} (UTC).",
            "processed_file": new_file,
            "path": path,
            "num_files": len(files),
            "updated_list": files,
        },
    )
    return new_path, files
