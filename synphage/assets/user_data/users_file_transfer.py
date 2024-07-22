from dagster import (
    AssetSpec,
    asset,
    multi_asset,
    MaterializeResult,
    AssetExecutionContext,
)

import os
import shutil
import pickle

from pathlib import Path
from collections import namedtuple
from synphage.resources.local_resource import OWNER


UsersRecord = namedtuple("UsersRecord", "new,history")


@multi_asset(
    required_resource_keys={"local_resource"},
    description="Check for the presence of files in the input_folder. If files are present, the pipeline will run. If no files are present the pipeline will stop.",
    specs=[AssetSpec("empty", skippable=True), AssetSpec("files", skippable=True)],
)
def has_files(context: AssetExecutionContext):
    users_dir = context.resources.local_resource.get_paths()["USER_DATA"]
    is_empty = not bool({file for file in Path(users_dir).glob("*.gb*")})

    if is_empty is True:
        yield MaterializeResult(asset_key="empty")

    else:
        yield MaterializeResult(asset_key="files")


@asset(
    deps=["empty"],
    description="Empty directory warning.",
    compute_kind="Python",
    metadata={"owner": OWNER},
)
def empty_dir(context) -> str:
    context.add_output_metadata(
        metadata={
            "text": "No genbank files were found in the input_folder. Please add genbank files to the input folder or check the input folder path.",
        },
    )
    return "This directory is empty"


@asset(
    deps=["files"],
    required_resource_keys={"local_resource"},
    description="Transfer user's files to the genbank folder and harmonise naming of the files",
    compute_kind="Python",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def users_to_genbank(context) -> UsersRecord:
    users_dir = context.resources.local_resource.get_paths()["USER_DATA"]
    _gb_files = [str(file) for file in Path(users_dir).glob("*.gb*")]
    # Check if history of transferred files
    fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
    _path_history = Path(fs) / "users_to_genbank"

    if os.path.exists(_path_history):
        _history_files = pickle.load(open(_path_history, "rb")).history
        context.log.info("Transferred file history loaded")
    else:
        _history_files = []
        context.log.info("No transfer history")

    # Transfer only new files
    _T = list(set(_gb_files).difference(set(_history_files)))
    context.log.info(f"Number of genomes to transfer: {len(_T)}")

    # Path to genbank folder
    _gb_path = context.resources.local_resource.get_paths()["GENBANK_DIR"]
    os.makedirs(_gb_path, exist_ok=True)

    # Harmonise file name
    _new_transfer = []
    for _file in _T:
        _output_file = str(
            Path(_gb_path)
            / f"{Path(_file).stem.replace('.', '_').replace(' ', '_')}.gb"
        )
        shutil.copy2(
            _file,
            _output_file,
        )
        context.log.info(f"{_file} transferred")
        _new_transfer.append(Path(_output_file).name)
        _history_files.append(_file)

    context.log.info("Transfer completed")

    context.add_output_metadata(
        metadata={
            "path": _gb_path,
            "num_new_files": len(_new_transfer),
            "new_files_preview": _new_transfer,
            "total_files": len(_history_files),
            "total_files_preview": _history_files,
        },
    )
    return UsersRecord(_new_transfer, _history_files)
