from dagster import (
    asset,
    Field,
    multi_asset,
    AssetOut,
    EnvVar,
)

import os
import pickle

from Bio import SeqIO
from pathlib import Path
from datetime import datetime
from typing import List


file_config = {
    "fs": Field(
        str,
        description="Path to folder containing the genbank _files",
        default_value="fs",
    ),
}

sqc_folder_config = {
    "sqc_download_dir": Field(
        str,
        description="Path to folder containing the downloaded genbank sequences",
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
        default_value="gene_identity/fasta",
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
    config_schema={**sqc_folder_config, **file_config},
    outs={
        "new_fasta_files": AssetOut(
            is_required=True,
            description="""Return the path for last created fasta _file. Parse genebank _file and create a _file containing every genes in the fasta format.
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
    op_tags={"blaster": "compute_intense"},
)
def genbank_to_fasta(context, standardised_ext_file):
    # Paths to read and store the data
    _path_out = (
        Path(os.getenv(EnvVar("PHAGY_DIRECTORY"))) / context.op_config["fasta_dir"]
    )

    _path = (
        Path(os.getenv(EnvVar("PHAGY_DIRECTORY")))
        / context.op_config["fs"]
        / "history_fasta_files"
    )

    # fasta_history
    if os.path.exists(_path):
        _fasta_files = pickle.load(open(_path, "rb"))
    else:
        _fasta_files = []
    context.log.info(_fasta_files)

    context.log.info(_path_out)

    os.makedirs(_path_out, exist_ok=True)

    _new_fasta_files = []
    _new_fasta_paths = []
    for _file in standardised_ext_file:
        if Path(_file).stem not in _fasta_files:
            context.log.info(f"The following file {_file} is being processed")

            # Genbank to fasta
            _output_dir = f"{_path_out}/{Path(_file).stem}.fna"
            context.log.info(_output_dir)
            _genome = SeqIO.read(_file, "genbank")
            _genome_records = list(SeqIO.parse(_file, "genbank"))

            if _assess_file_content(_genome) == True:
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
                                    _feature.qualifiers["locus_tag"][0],
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

    context.log.info(_fasta_files)

    _fasta_files = _fasta_files + _new_fasta_files

    _time = datetime.now()
    context.add_output_metadata(
        output_name="new_fasta_files",
        metadata={
            "text_metadata": f"The list of fasta files has been updated {_time.isoformat()} (UTC).",
            "processed_file": _new_fasta_files,
            "num_files": len(_new_fasta_files),
            "path": _path_out,
        },
    )
    context.add_output_metadata(
        output_name="history_fasta_files",
        metadata={
            "text_metadata": f"The list of fasta files has been updated {_time.isoformat()} (UTC).",
            "path": _path_out,
            "num_files": len(_fasta_files),
            "preview": _fasta_files,
        },
    )

    return _new_fasta_paths, _fasta_files


blastn_folder_config = {
    "blast_db_dir": Field(
        str,
        description="Path to folder containing the database for the blastn",
        default_value="gene_identity/blastn_database",
    ),
    "blastn_dir": Field(
        str,
        description="Path to folder containing the blastn output _files",
        default_value="gene_identity/blastn",
    ),
}


@asset(
    config_schema={**sqc_folder_config, **blastn_folder_config},
    description="Receive a fasta _file as input and create a database for blast in the output directory",
    compute_kind="Blastn",
    op_tags={"blaster": "compute_intense"},
    metadata={"owner": "Virginie Grosboillot"},
)
def create_blast_db(context, new_fasta_files):
    _path = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["blast_db_dir"]]
    )
    context.log.info(_path)
    os.makedirs(_path, exist_ok=True)

    _db = []
    for _new_fasta_file in new_fasta_files:
        _output_dir = f"{_path}/{Path(_new_fasta_file).stem}"
        context.log.info(_output_dir)
        os.system(
            f"makeblastdb -in {_new_fasta_file} -input_type fasta -dbtype nucl -out {_output_dir}"
        )
        context.log.info("finished process")
        _db.append(_new_fasta_file)

    _all_db = list(set(map(lambda x: f"{_path}/{Path(x).stem}", os.listdir(_path))))

    _time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"The list of dbs has been updated {_time.isoformat()} (UTC).",
            "processed_files": _db,
            "path": _path,
            "preview": list(set([Path(_p).stem for _p in _all_db])),
        }
    )

    return _all_db


@asset(
    config_schema={**sqc_folder_config, **blastn_folder_config},
    description="Perform blastn between sequence and database and return results as json",
    compute_kind="Blastn",
    op_tags={"blaster": "compute_intense"},
    metadata={"owner": "Virginie Grosboillot"},
)
def get_blastn(context, history_fasta_files, create_blast_db):
    # Blastn json _file directory - create directory if not yet existing
    _path = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["blastn_dir"]]
    )
    os.makedirs(_path, exist_ok=True)
    context.log.info(_path)

    # History
    _history_path = "/".join(
        [os.getenv("PHAGY_DIRECTORY"), os.getenv("FILE_SYSTEM"), "get_blastn"]
    )
    context.log.info(_history_path)
    if os.path.exists(_history_path):
        context.log.info("path exist")
        _blastn_history = [
            Path(file).name for file in pickle.load(open(_history_path, "rb"))
        ]
    else:
        context.log.info("path do not exist")
        _blastn_history = []
    context.log.info(_blastn_history)

    # Blast each query against every databases
    _fasta_path = "/".join(
        [os.getenv("PHAGY_DIRECTORY"), context.op_config["fasta_dir"]]
    )
    context.log.info(_fasta_path)
    _fasta_files = list(map(lambda x: f"{_fasta_path}/{x}.fna", history_fasta_files))

    for _query in _fasta_files:
        for _database in create_blast_db:
            _output_dir = f"{_path}/{Path(_query).stem}_vs_{Path(_database).stem}"
            if Path(_output_dir).name not in _blastn_history:
                context.log.info(f"output_dir {_output_dir} not in _blastn_history")
                os.system(
                    f"blastn -query {_query} -db {_database} -evalue 1e-3 -dust no -out {_output_dir} -outfmt 15"
                )
                _blastn_history.append(_output_dir)
                context.log.info(f"{Path(_output_dir)} processed successfully")

    _full_list = [Path(_x).stem for _x in _blastn_history]

    # Asset metadata
    _time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"The list of blasted sequneces has been updated {_time.isoformat()} (UTC).",
            "processed__files": _full_list,
            "path": _path,
        }
    )

    return _blastn_history
