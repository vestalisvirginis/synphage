from dagster import asset

import os
import pickle
import polars as pl

from pathlib import Path
from typing import List
from collections import namedtuple
from synphage.resources.local_resource import OWNER


FastaPRecord = namedtuple("FastaPRecord", "new,history")
BlastPRecord = namedtuple("BlastPRecord", "new,history")


@asset(
    required_resource_keys={"local_resource"},
    description="Create a fasta file of amino acid sequences for each dataset",
    compute_kind="Python",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def create_fasta_p(context, append_processed_df) -> FastaPRecord:

    # Path to genbank folder
    _fasta_path = context.resources.local_resource.get_paths()["FASTA_P_DIR"]
    os.makedirs(_fasta_path, exist_ok=True)

    # Check if history of created fasta files
    fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
    _path_history = Path(fs) / "create_fasta_p"

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
                    ">%s \n%s\n"
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


@asset(
    required_resource_keys={"local_resource"},
    description="Uses makeblastdb to produce protein BLAST databases from the fasta files",
    io_manager_key="io_manager",
    compute_kind="Blastp",
    metadata={"owner": OWNER},
)
def create_blast_p_db(context, create_fasta_p) -> List[str]:
    # path to store db
    _path_db = context.resources.local_resource.get_paths()["BLASTP_DB_DIR"]
    context.log.info(f"Path to database: {_path_db}")
    os.makedirs(_path_db, exist_ok=True)

    _db = []
    for _new_fasta_file in create_fasta_p.new:
        _output_dir = str(Path(_path_db) / Path(_new_fasta_file).stem)
        context.log.info(f"Database being generated: {_output_dir}")
        os.system(
            f"makeblastdb -in {_new_fasta_file} -input_type fasta -dbtype prot -out {_output_dir}"
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
    description="Perform blastp between available sequences and databases and return result in json format",
    io_manager_key="io_manager",
    compute_kind="blastp",
    metadata={"owner": OWNER},
)
def get_blastp(context, create_fasta_p, create_blast_p_db) -> BlastPRecord:
    # blastp json file directory - create directory if not yet existing
    _path_blastp = context.resources.local_resource.get_paths()["BLASTP_DIR"]
    os.makedirs(_path_blastp, exist_ok=True)
    context.log.info(f"Path to blastp results: {_path_blastp}")

    # History
    fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
    _history_path = str(Path(fs) / "get_blastp")
    if os.path.exists(_history_path):
        _blastp_history = [
            Path(file).name for file in pickle.load(open(_history_path, "rb")).history
        ]
        context.log.info("Blastp history loaded")
    else:
        _blastp_history = []
        context.log.info("No blastp history available")

    # Blast each query against every databases
    _fasta_files = create_fasta_p.history

    _new_blastp_files = []
    for _query in _fasta_files:
        for _database in create_blast_p_db:
            if not Path(_query).stem == Path(_database).stem:
                _output_file = f"{Path(_query).stem}_vs_{Path(_database).stem}"
                _output_dir = str(Path(_path_blastp) / _output_file)
                if _output_file not in _blastp_history:
                    os.system(
                        f"blastp -query {_query} -db {_database} -evalue 1e-3 -out {_output_dir} -outfmt 15"
                    )
                    _new_blastp_files.append(_output_file)
                    _blastp_history.append(_output_dir)
                    context.log.info(f"Query {_output_file} processed successfully")

    _full_list = [Path(x).stem for x in _blastp_history]

    # Asset metadata
    context.add_output_metadata(
        metadata={
            "file_location": _path_blastp,
            "num_files": len(_new_blastp_files),
            "processed_files": _new_blastp_files,
            "preview_all": _full_list,
        }
    )

    return BlastPRecord(_new_blastp_files, _full_list)
