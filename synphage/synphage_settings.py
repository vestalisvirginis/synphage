import os
import tempfile

from pathlib import Path
from dagster import EnvVar

TEMP_DIR = tempfile.gettempdir()
USER_DATA = str(Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR)))
SYNPHAGE_DATA = str(Path(os.getenv(EnvVar("OUTPUT_DIR"), TEMP_DIR)))

# main folders
# Path to folder containing the download gb files from the ncbi database
DOWNLOAD_DIR = str(Path(SYNPHAGE_DATA) / "download")
# Path to folder containing the genbank files
GENBANK_DIR = str(Path(SYNPHAGE_DATA) / "genbank")
# Path to folder containing the file system files
FILESYSTEM_DIR = str(Path(SYNPHAGE_DATA) / "fs")
# Path to folder containing the blastn related folders
GENE_DIR = str(Path(SYNPHAGE_DATA) / "gene_identity")
# Path to folder containing the blastp related folders
PROTEIN_DIR = str(Path(SYNPHAGE_DATA) / "protein_identity")
# Path to folder containing the data tables
TABLES_DIR = str(Path(SYNPHAGE_DATA) / "tables")
# Path to folder containing the plot files
SYNTENY_DIR = str(Path(SYNPHAGE_DATA) / "synteny")

# blastn related folders
# Path to folder containing the fasta files
FASTA_N_DIR = str(Path(GENE_DIR) / "fasta_n")
# Path to folder containing the database to perform the blastn
BLASTN_DB_DIR = str(Path(GENE_DIR) / "blastn_database")
# Path to folder containing the blastn json files
BLASTN_DIR = str(Path(GENE_DIR) / "blastn")

# blastp related folders
# Path to folder containing the fasta files
FASTA_P_DIR = str(Path(PROTEIN_DIR) / "fasta_p")
# Path to folder containing the database to perform the blastp
BLASTP_DB_DIR = str(Path(PROTEIN_DIR) / "blastp_database")
# Path to folder containing the blastp json files
BLASTP_DIR = str(Path(PROTEIN_DIR) / "blastp")
