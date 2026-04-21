from dagster import asset

import os
import pickle
import polars as pl

from pathlib import Path
from collections import namedtuple
from synphage.resources.local_resource import OWNER

FastaPRecord = namedtuple("FastaPRecord", "new,history")


@asset(
    required_resource_keys={"local_resource"},
    description="Create a fasta file of amino acid sequences for each dataset (foldseek workflow)",
    compute_kind="Python",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def foldseek_create_fasta_p(context, append_processed_df) -> FastaPRecord:
    # Path to fasta folder
    _fasta_path = context.resources.local_resource.get_paths()["FASTA_P_DIR"]
    os.makedirs(_fasta_path, exist_ok=True)

    # Check if history of created fasta files
    fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
    _path_history = Path(fs) / "foldseek_create_fasta_p"

    if os.path.exists(_path_history):
        _history_files = pickle.load(open(_path_history, "rb")).history
        context.log.info("Transferred file history loaded")
    else:
        _history_files = []
        context.log.info("No transfer history")

    # Write fasta for new files only
    df, seq, check_df = append_processed_df

    listed_files = [
        str(Path(_fasta_path) / f"{Path(path).stem}.fna")
        for path in df.select("filename").unique().to_series().to_list()
    ]

    _T = list(set(listed_files).difference(set(_history_files)))
    context.log.info(f"Number of genomes to convert to fasta: {len(_T)}")

    # Create fasta files
    _new_fasta = []
    _new_file = []
    for _file in _T:
        context.log.info(f"The following file {Path(_file).name} is being processed")

        with open(_file, "w") as _f:
            for data in (
                df.filter(pl.col("filename").str.contains(Path(_file).stem))
                .select("key", "translation_fn")
                .iter_rows(named=True)
            ):
                _f.write(
                    ">f_%s \n%s\n"
                    % (
                        data["key"],
                        data["translation_fn"],
                    )
                )
        context.log.info(f"Fasta for {Path(_file).name} created")
        _new_fasta.append(_file)
        _new_file.append(Path(_file).name)
        _history_files.append(_file)

    context.log.info("Fasta file creation completed.")

    context.add_output_metadata(
        metadata={
            "path": _fasta_path,
            "num_new_files": len(_new_file),
            "new_files_preview": _new_file,
            "total_files": len(_history_files),
            "total_files_preview": _history_files,
        },
    )

    return FastaPRecord(_new_fasta, _history_files)
