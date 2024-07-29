from dagster import asset, ExperimentalWarning

import os
import pickle
import warnings
import polars as pl

from pathlib import Path
from typing import List
from collections import namedtuple
from synphage.resources.local_resource import OWNER

warnings.filterwarnings("ignore", category=ExperimentalWarning)


FastaNRecord = namedtuple("FastaNRecord", "new,history")
BlastNRecord = namedtuple("BlastNRecord", "new,history")


@asset(
    required_resource_keys={"local_resource"},
    description="Create a fasta file of nucleotide sequences for each dataset",
    compute_kind="Python",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def create_fasta_n(context, append_processed_df) -> FastaNRecord:

    # Path to fasta folder
    _fasta_path = context.resources.local_resource.get_paths()["FASTA_N_DIR"]
    os.makedirs(_fasta_path, exist_ok=True)

    # Check if history of created fasta files
    fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
    _path_history = Path(fs) / "create_fasta_n"

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
    _new_files = []
    for _file in _T:
        context.log.info(f"The following file {Path(_file).name} is being processed")

        with open(_file, "w") as _f:
            for data in (
                df.filter(pl.col("filename").str.contains(Path(_file).stem))
                .select("key", "extract")
                .iter_rows(named=True)
            ):
                _f.write(
                    ">%s \n%s\n"
                    % (
                        data["key"],
                        data["extract"],
                    )
                )
        context.log.info(f"Fasta for {Path(_file).name} created")
        _new_fasta.append(_file)
        _new_files.append(Path(_file).name)
        _history_files.append(_file)

    context.log.info("Fasta file creation completed.")

    context.add_output_metadata(
        metadata={
            "path": _fasta_path,
            "num_new_files": len(_new_files),
            "new_files_preview": _new_files,
            "total_files": len(_history_files),
            "total_files_preview": _history_files,
        },
    )

    return FastaNRecord(_new_fasta, _history_files)


@asset(
    required_resource_keys={"local_resource"},
    description="Uses makeblastdb to produce nucleotide BLAST databases from the fasta files",
    io_manager_key="io_manager",
    compute_kind="Blastn",
    metadata={"owner": OWNER},
)
def create_blast_n_db(context, create_fasta_n) -> List[str]:
    # path to store db
    _path_db = context.resources.local_resource.get_paths()["BLASTN_DB_DIR"]
    context.log.info(f"Path to database: {_path_db}")
    os.makedirs(_path_db, exist_ok=True)

    _db = []
    for _new_fasta_file in create_fasta_n.new:
        _output_dir = str(Path(_path_db) / Path(_new_fasta_file).stem)
        context.log.info(f"Database being generated: {_output_dir}")
        os.system(
            f"makeblastdb -in {_new_fasta_file} -input_type fasta -dbtype nucl -out {_output_dir}"
        )
        _db.append(_new_fasta_file)
        context.log.info(f"File {_new_fasta_file} has been processed")

    _all_db = list(
        set(map(lambda x: str(Path(_path_db) / Path(x).stem), os.listdir(_path_db)))
    )

    context.add_output_metadata(
        metadata={
            "file_location": _path_db,
            "num_new_files": len(_db),
            "processed_files": _db,
            "preview_all": list(set([Path(_p).stem for _p in _all_db])),
        }
    )

    return _all_db


@asset(
    required_resource_keys={"local_resource"},
    description="Perform blastn between available sequences and databases and return result in json format",
    io_manager_key="io_manager",
    compute_kind="Blastn",
    metadata={"owner": OWNER},
)
def get_blastn(context, create_fasta_n, create_blast_n_db) -> BlastNRecord:
    # Blastn json file directory - create directory if not yet existing
    _path_blastn = context.resources.local_resource.get_paths()["BLASTN_DIR"]
    os.makedirs(_path_blastn, exist_ok=True)
    context.log.info(f"Path to blastn results: {_path_blastn}")

    # History
    fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
    _history_path = str(Path(fs) / "get_blastn")
    if os.path.exists(_history_path):
        _blastn_history = [
            Path(file).name for file in pickle.load(open(_history_path, "rb")).history
        ]
        context.log.info("Blastn history loaded")
    else:
        _blastn_history = []
        context.log.info("No blastn history available")

    # Blast each query against every databases
    _fasta_files = create_fasta_n.history

    _new_blastn_files = []
    for _query in _fasta_files:
        for _database in create_blast_n_db:
            if not Path(_query).stem == Path(_database).stem:
                _output_file = f"{Path(_query).stem}_vs_{Path(_database).stem}"
                _output_dir = str(Path(_path_blastn) / _output_file)
                if _output_file not in _blastn_history:
                    os.system(
                        f"blastn -query {_query} -db {_database} -evalue 1e-3 -dust no -out {_output_dir} -outfmt 15"
                    )
                    _new_blastn_files.append(_output_file)
                    _blastn_history.append(_output_dir)
                    context.log.info(f"Query {_output_file} processed successfully")

    _full_list = [Path(x).stem for x in _blastn_history]

    # Asset metadata
    context.add_output_metadata(
        metadata={
            "file_location": _path_blastn,
            "num_files": len(_new_blastn_files),
            "processed_files": _new_blastn_files,
            "preview_all": _full_list,
        }
    )

    return BlastNRecord(_new_blastn_files, _full_list)
