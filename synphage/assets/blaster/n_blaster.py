from dagster import (
    asset,
    Field,
    multi_asset,
    AssetOut,
    EnvVar,
)

import os
import pickle
import tempfile

from Bio import SeqIO
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
    "genbank_dir": Field(
        str,
        description="Path to folder containing the genbank files",
        default_value="genbank",
    ),
    "fasta_dir": Field(
        str,
        description="Path to folder containing the fasta files",
        default_value=str(Path("gene_identity") / "fasta"),
    ),
    "blast_db_dir": Field(
        str,
        description="Path to folder containing the database for the blastn",
        default_value=str(Path("gene_identity") / "blastn_database"),
    ),
    "blastn_dir": Field(
        str,
        description="Path to folder containing the blastn json files",
        default_value=str(Path("gene_identity") / "blastn"),
    ),
}


def _assess_file_content(genome) -> bool:
    """Assess wether the genbank file contains gene or only CDS"""

    gene_count = 0
    gene_value = False
    for feature in genome.features:
        if feature.type == "gene":
            gene_count = gene_count + 1
            if gene_count > 1:
                gene_value = True
                break

    return gene_value


@multi_asset(
    config_schema=folder_config,
    outs={
        "new_fasta_files": AssetOut(
            is_required=True,
            description="""Return the path for last created fasta files. Parse genebank file and create a file containing every genes in the fasta format.
            Note: The sequence start and stop indexes are `-1` on the fasta _file 1::10  --> [0:10] included/excluded.""",
            io_manager_key="io_manager",
            metadata={
                "owner": "Virginie Grosboillot",
            },
        ),
        "history_fasta_files": AssetOut(
            is_required=True,
            description="Update the list of sequences available in the fasta folder",
            io_manager_key="io_manager",
            metadata={
                "owner": "Virginie Grosboillot",
            },
        ),
    },
    compute_kind="Biopython",
)
def genbank_to_fasta(context, standardised_ext_file) -> tuple[List[str], List[str]]:
    # Paths to read from and store the data
    _path_fasta = str(
        Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR)) / context.op_config["fasta_dir"]
    )

    _path_history = (
        Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR))
        / context.op_config["fs"]
        / "history_fasta_files"
    )

    # fasta_history
    if os.path.exists(_path_history):
        _fasta_files = pickle.load(open(_path_history, "rb"))
        context.log.info("Fasta history loaded")
    else:
        _fasta_files = []
        context.log.info("No fasta history")

    context.log.info(f"Path to fasta files: {_path_fasta}")
    os.makedirs(_path_fasta, exist_ok=True)

    _new_fasta_files = []
    _new_fasta_paths = []
    for _file in standardised_ext_file:
        if Path(_file).stem not in _fasta_files:
            context.log.info(f"The following file {_file} is being processed")

            # Genbank to fasta
            _output_dir = str(Path(_path_fasta) / f"{Path(_file).stem}.fna")
            context.log.info(f"Output file being generated: {_output_dir}")
            _genome = SeqIO.read(_file, "genbank")
            _genome_records = list(SeqIO.parse(_file, "genbank"))

            if _assess_file_content(_genome):
                with open(_output_dir, "w") as _f:
                    _gene_features = list(
                        filter(lambda x: x.type == "gene", _genome.features)
                    )
                    for _feature in _gene_features:
                        for _seq_record in _genome_records:
                            _f.write(
                                ">%s | %s | %s | %s | %s | %s\n%s\n"
                                % (
                                    _seq_record.name,
                                    _seq_record.id,
                                    _seq_record.description,
                                    _feature.qualifiers["gene"][0]
                                    if "gene" in _feature.qualifiers.keys()
                                    else "None",
                                    _feature.qualifiers["locus_tag"][0]
                                    if "locus_tag" in _feature.qualifiers.keys()
                                    else "None",
                                    _feature.location,
                                    _seq_record.seq[
                                        _feature.location.start : _feature.location.end
                                    ],
                                )
                            )
            else:
                with open(_output_dir, "w") as _f:
                    _gene_features = list(
                        filter(lambda x: x.type == "CDS", _genome.features)
                    )
                    for _feature in _gene_features:
                        for _seq_record in _genome_records:
                            _f.write(
                                ">%s | %s | %s | %s | %s | %s\n%s\n"
                                % (
                                    _seq_record.name,
                                    _seq_record.id,
                                    _seq_record.description,
                                    _feature.qualifiers["protein_id"][0],
                                    _feature.qualifiers["protein_id"][0][:-2],
                                    _feature.location,
                                    _seq_record.seq[
                                        _feature.location.start : _feature.location.end
                                    ],
                                )
                            )

            _new_fasta_files.append(Path(_file).stem)
            _new_fasta_paths.append(_output_dir)

    _fasta_files = _fasta_files + _new_fasta_files
    context.log.info("The fasta file history has been updated")

    _time = datetime.now()
    context.add_output_metadata(
        output_name="new_fasta_files",
        metadata={
            "text_metadata": f"New genbank files have been processed to fasta {_time.isoformat()} (UTC).",
            "file_location": _path_fasta,
            "num_processed_files": len(_new_fasta_files),
            "preview": _new_fasta_files,
        },
    )
    context.add_output_metadata(
        output_name="history_fasta_files",
        metadata={
            "text_metadata": f"The list of fasta files has been updated {_time.isoformat()} (UTC).",
            "file_location": _path_history,
            "num_files": len(_fasta_files),
            "preview": _fasta_files,
        },
    )

    return _new_fasta_paths, _fasta_files


@asset(
    config_schema=folder_config,
    description="Receive a fasta file as input and create a database for the blastn step",
    compute_kind="Blastn",
    metadata={"owner": "Virginie Grosboillot"},
)
def create_blast_db(context, new_fasta_files) -> List[str]:
    # path to store db
    _path_db = str(
        Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR))
        / context.op_config["blast_db_dir"]
    )
    context.log.info(f"Path to database: {_path_db}")
    os.makedirs(_path_db, exist_ok=True)

    _db = []
    for _new_fasta_file in new_fasta_files:
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

    _time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"The list of dbs has been updated {_time.isoformat()} (UTC).",
            "file_location": _path_db,
            "num_new_files": len(_db),
            "processed_files": _db,
            "preview_all": list(set([Path(_p).stem for _p in _all_db])),
        }
    )

    return _all_db


@asset(
    config_schema=folder_config,
    description="Perform blastn between available sequences and databases and return result in json format",
    compute_kind="Blastn",
    metadata={"owner": "Virginie Grosboillot"},
)
def get_blastn(context, history_fasta_files, create_blast_db) -> List[str]:
    # Blastn json file directory - create directory if not yet existing
    _path_blastn = str(
        Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR)) / context.op_config["blastn_dir"]
    )
    os.makedirs(_path_blastn, exist_ok=True)
    context.log.info(f"Path to blastn results: {_path_blastn}")

    # History
    _history_path = str(
        Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR)) / "fs" / "get_blastn"
    )
    if os.path.exists(_history_path):
        _blastn_history = [
            Path(file).name for file in pickle.load(open(_history_path, "rb"))
        ]
        context.log.info("Blastn history loaded")
    else:
        _blastn_history = []
        context.log.info("No blastn history available")

    # Blast each query against every databases
    _fasta_path = str(
        Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR)) / context.op_config["fasta_dir"]
    )
    context.log.info(f"Path to fasta files: {_fasta_path}")
    _fasta_files = list(
        map(lambda x: str(Path(_fasta_path) / f"{x}.fna"), history_fasta_files)
    )

    _new_blastn_files = []
    for _query in _fasta_files:
        for _database in create_blast_db:
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
    _time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"The list of blasted sequences has been updated {_time.isoformat()} (UTC).",
            "file_location": _path_blastn,
            "num_files": len(_new_blastn_files),
            "processed_files": _new_blastn_files,
            "preview_all": _full_list,
        }
    )

    return _blastn_history
