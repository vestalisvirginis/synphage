from dagster import asset

import os
import shutil
import pickle

from pathlib import Path
from collections import namedtuple

from synphage.synphage_settings import FILESYSTEM_DIR, GENBANK_DIR

DownloadRecord = namedtuple("DownloadRecord", "new,history")


@asset(
    description="Transfer new downloaded files to the genbank folder and harmonise naming of the files",
    compute_kind="Python",
    metadata={"owner": "Virginie Grosboillot"},
)
def download_to_genbank(context, fetch_genome) -> DownloadRecord:
    # Check if history of transferred files
    _path_history = Path(FILESYSTEM_DIR) / "download_to_genbank"

    if os.path.exists(_path_history):
        _history_files = pickle.load(open(_path_history, "rb")).history
        context.log.info("Transferred file history loaded")
    else:
        _history_files = []
        context.log.info("No transfer history")

    # Transfer only new files
    _T = list(set(fetch_genome).difference(set(_history_files)))
    context.log.info(f"Number of genomes to transfer: {len(_T)}")

    # Path to genbank folder
    _gb_path = GENBANK_DIR
    os.makedirs(_gb_path, exist_ok=True)

    # Harmonise file name
    _new_transfer = []
    for _file in _T:
        _output_file = str(Path(_gb_path) / f"{Path(_file).stem.replace('.', '_')}.gb")
        shutil.copy2(
            _file,
            _output_file,
        )
        _new_transfer.append(Path(_output_file).name)
        _history_files.append(_file)

    context.add_output_metadata(
        metadata={
            "path": _gb_path,
            "num_new_files": len(_new_transfer),
            "new_files_preview": _new_transfer,
            "total_files": len(_history_files),
            "total_files_preview": _history_files,
        },
    )

    return DownloadRecord(_new_transfer, _history_files)
