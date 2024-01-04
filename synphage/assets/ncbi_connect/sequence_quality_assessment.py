from dagster import (
    asset,
    EnvVar,
    Field,
    multi_asset,
    AssetOut,
)

import os
import shutil
import tempfile
import pickle

from pathlib import Path
from datetime import datetime
from typing import List


TEMP_DIR = tempfile.gettempdir()


folder_config = {
    "fs": Field(
        str,
        description="Path to folder containing the file system files",
        default_value="fs",
    ),
    "download_directory": Field(
        str,
        description="Path to folder",
        default_value="download",
    ),
    "genbank_dir": Field(
        str,
        description="Path to folder containing the genbank files",
        default_value="genbank",
    ),
    "fasta_dir": Field(
        str,
        description="Path to folder containing the fasta sequence files",
        default_value=str(Path("gene_identity") / "fasta"),
    ),
}


@multi_asset(
    config_schema=folder_config,
    outs={
        "new_transferred_files": AssetOut(
            is_required=True,
            description="Checks for sequence quality and accuracy",
            io_manager_key="io_manager",
            metadata={
                "owner": "Virginie Grosboillot",
            },
        ),
        "history_transferred_files": AssetOut(
            is_required=True,
            description="Update the list of files already transferred from download to genbank folder",
            io_manager_key="io_manager",
            metadata={
                "owner": "Virginie Grosboillot",
            },
        ),
    },
    compute_kind="Biopython",
)
def sequence_check(context, fetch_genome) -> tuple[List[str], List[str]]:
    # history check
    _path_history = (
        Path(os.getenv(EnvVar("DATA_DIR")))
        / context.op_config["fs"]
        / "history_transferred_files"
    )

    if os.path.exists(_path_history):
        _transferred_files = pickle.load(open(_path_history, "rb"))
        context.log.info("Transferred file history loaded")
    else:
        _transferred_files = []
        context.log.info("No transfer history")

    # genome to transfer
    _T = list(set(fetch_genome).difference(set(_transferred_files)))
    context.log.info(f"Number of genomes to transfer: {len(_T)}")

    # path to genbank folder
    _gb_path = "/".join(
        [os.getenv(EnvVar("DATA_DIR")), context.op_config["genbank_dir"]]
    )
    os.makedirs(_gb_path, exist_ok=True)

    # add check to assess the quality of the query

    _new_transfer = []
    for _file in _T:
        _output_file = f"{_gb_path}/{Path(_file).stem.replace('.', '_')}.gb"
        shutil.copy2(
            _file,
            _output_file,
        )
        _new_transfer.append(Path(_output_file).name)
        _transferred_files.append(_file)

    _time = datetime.now()
    context.add_output_metadata(
        output_name="new_transferred_files",
        metadata={
            "text_metadata": f"List of downloaded sequences{_time.isoformat()} (UTC).",
            "path": _gb_path,
            "num_files": len(_new_transfer),
            "preview": _new_transfer,
        },
    )
    context.add_output_metadata(
        output_name="history_transferred_files",
        metadata={
            "text_metadata": f"List of downloaded sequences last update: {_time.isoformat()} (UTC).",
            "num_files": len(_transferred_files),
            "preview": _transferred_files,
        },
    )

    return _new_transfer, _transferred_files
