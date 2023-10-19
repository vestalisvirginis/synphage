from dagster import (
    asset,
    Field,
    multi_asset,
    AssetOut,
    EnvVar,
)

import os
import re
import shutil
import pickle

from Bio import SeqIO
from pathlib import Path
from datetime import datetime
from typing import List


genbank_folder_config = {
    "phage_download_directory": Field(
        str,
        description="Path to folder containing the genebank sequence files",
        # default_value="/usr/src/data/phage_view_data/genome_download",
        default_value="/usr/src/data_folder/jaka_data/genome_download",
    ),
    "spbetaviruses_directory": Field(
        str,
        description="Path to folder containing the genebank sequence files",
        # default_value="/usr/src/data_folder/phage_view_data/genbank_spbetaviruses",
        default_value="/usr/src/data_folder/jaka_data/genbank",
    ),
}


@asset(
    config_schema={**genbank_folder_config},
    # config_schema={**path_config},
    description="Select for Spbetaviruses with complete genome sequence",
    compute_kind="Biopython",
    metadata={"owner": "Virginie Grosboillot"},
)
def sequence_sorting(context, fetch_genome) -> List[str]:
    context.log.info(f"Number of genomes in download folder: {len(fetch_genome)}")
    # context.log.info(f"Files: {fetch_genome}")

    complete_sequences = []
    for file in fetch_genome:
        for p in SeqIO.parse(file, "gb"):
            if re.search("complete genome", p.description):
                complete_sequences.append(file)

    context.log.info(f"Number of complete sequences: {len(complete_sequences)}")

    bacillus_sub_sequences = []
    for file in complete_sequences:
        for p in SeqIO.parse(file, "gb"):
            for feature in p.features:
                if feature.type == "source":
                    for v in feature.qualifiers.values():
                        if re.search("Bacillus subtilis", v[0]):
                            bacillus_sub_sequences.append(file)

    context.log.info(
        f"Number of Bacillus subtilis sequences: {len(bacillus_sub_sequences)}"
    )

    genes_in_sequences = []
    for file in bacillus_sub_sequences:
        for p in SeqIO.parse(file, "gb"):
            if set(["gene"]).issubset(set([type_f.type for type_f in p.features])):
                genes_in_sequences.append(file)

    context.log.info(
        f"Number of sequences with gene features: {len(genes_in_sequences)}"
    )

    for file in genes_in_sequences:
        shutil.copy2(
            file, f'{context.op_config["spbetaviruses_directory"]}/{Path(file).stem}.gb'
        )

    return list(
        map(
            lambda x: Path(x).stem,
            os.listdir(context.op_config["spbetaviruses_directory"]),
        )
    )


file_config = {
    "fs": Field(
        str,
        description="Path to folder containing the genbank files",
        default_value="fs",
    ),
}

sqc_folder_config = {
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
        "new_fasta_file": AssetOut(
            is_required=True,
            description="""Return the path for last created fasta file. Parse genebank file and create a file containing every genes in the fasta format.
            Note: The sequence start and stop indexes are `-1` on the fasta file 1::10  --> [0:10] included/excluded.""",
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
def genbank_to_fasta(context, standardised_ext_file):  # -> str:
    context.log.info(
        f"The following file {Path(standardised_ext_file).stem} is being processed"
    )

    # Paths to read and store the data
    # path_in = "/".join(
    #     [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["genbank_dir"]]
    # )
    path_out = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["fasta_dir"]]
    )
    # context.log.info(path_in)
    context.log.info(path_out)

    os.makedirs(path_out, exist_ok=True)

    # Genbank to fasta
    file = standardised_ext_file
    context.log.info(file)
    output_dir = f"{path_out}/{Path(file).stem}.fna"
    context.log.info(output_dir)
    genome = SeqIO.read(file, "genbank")
    genome_records = list(SeqIO.parse(file, "genbank"))

    with open(output_dir, "w") as f:
        gene_features = list(filter(lambda x: x.type == "gene", genome.features))
        for feature in gene_features:
            for seq_record in genome_records:
                f.write(
                    ">%s | %s | %s | %s | %s | %s\n%s\n"
                    % (
                        seq_record.name,
                        seq_record.id,
                        seq_record.description,
                        feature.qualifiers["gene"][0]
                        if "gene" in feature.qualifiers.keys()
                        else "None",
                        feature.qualifiers["locus_tag"][0],
                        feature.location,
                        seq_record.seq[feature.location.start : feature.location.end],
                    )
                )

    fasta_files = list(
        map(
            lambda x: f"{path_out}/{x}",
            os.listdir(path_out),
        )
    )
    context.log.info(fasta_files)
    time = datetime.now()
    context.add_output_metadata(
        output_name="new_fasta_file",
        metadata={
            "text_metadata": f"The list of fasta files has been updated {time.isoformat()} (UTC).",
            "processed_file": Path(standardised_ext_file).stem,
            "path": path_out,
        },
    )
    context.add_output_metadata(
        output_name="history_fasta_files",
        metadata={
            "text_metadata": f"The list of fasta files has been updated {time.isoformat()} (UTC).",
            "processed_file": Path(standardised_ext_file).stem,
            "path": path_out,
            "num_files": len(fasta_files),
            "preview": fasta_files,
        },
    )

    return output_dir, fasta_files


blastn_folder_config = {
    "blast_db_dir": Field(
        str,
        description="Path to folder containing the database for the blastn",
        default_value="gene_identity/blastn_database",
    ),
    "blastn_dir": Field(
        str,
        description="Path to folder containing the blastn output files",
        default_value="gene_identity/blastn",
    ),
}


@asset(
    config_schema={**sqc_folder_config, **blastn_folder_config},
    description="Receive a fasta file as input and create a database for blast in the output directory",
    compute_kind="Blastn",
    # auto_materialize_policy=AutoMaterializePolicy.eager(),
    op_tags={"blaster": "compute_intense"},
    metadata={"owner": "Virginie Grosboillot"},
)
def create_blast_db(context, new_fasta_file):
    path = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["blast_db_dir"]]
    )
    context.log.info(path)
    os.makedirs(path, exist_ok=True)
    # db = []
    # for input in genbank_to_fasta:
    output_dir = f"{path}/{Path(new_fasta_file).stem}"
    context.log.info(output_dir)
    os.system(
        f"makeblastdb -in {new_fasta_file} -input_type fasta -dbtype nucl -out {output_dir}"
    )
    context.log.info("finished process")
    # db.append(output_dir)

    db = set(map(lambda x: f"{path}/{Path(x).stem}", os.listdir(path)))

    context.log.info(f"list of db: {set([Path(p).stem for p in db])}")

    time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"The list of dbs has been updated {time.isoformat()} (UTC).",
            "processed_file": Path(new_fasta_file).stem,
            "path": path,
        }
    )

    return db


@asset(
    config_schema={**blastn_folder_config},
    description="Perform blastn between sequence and database and return results as json",
    compute_kind="Blastn",
    op_tags={"blaster": "compute_intense"},
    metadata={"owner": "Virginie Grosboillot"},
)
def get_blastn(context, history_fasta_files, create_blast_db):
    # Blastn json file directory - create directory if not yet existing
    path = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["blastn_dir"]]
    )
    os.makedirs(path, exist_ok=True)

    # History
    history_path = "/".join(
        [os.getenv("PHAGY_DIRECTORY"), os.getenv("FILE_SYSTEM"), "get_blastn"]
    )
    if os.path.exists(history_path):
        context.log.info("path exist")
        blastn_history = pickle.load(open(history_path, "rb"))
    else:
        context.log.info("path do not exist")
        blastn_history = []
    context.log.info(blastn_history)

    # Blast each query against every databases
    fasta_files = history_fasta_files

    for query in fasta_files:
        for database in create_blast_db:
            output_dir = f"{path}/{Path(query).stem}_vs_{Path(database).stem}"
            if output_dir not in blastn_history:
                os.system(
                    f"blastn -query {query} -db {database} -evalue 1e-3 -dust no -out {output_dir} -outfmt 15"
                )
                blastn_history.append(output_dir)
                context.log.info(f"{Path(output_dir)} processed successfully")

    full_list = [Path(x).stem for x in blastn_history]

    # Asset metadata
    time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"The list of blasted sequneces has been updated {time.isoformat()} (UTC).",
            "processed_files": full_list,
            "path": path,
        }
    )

    return blastn_history
