import os
import tempfile

from pathlib import Path
from dagster import ConfigurableResource, FilesystemIOManager
from pydantic import Field, field_validator


TEMP_DIR = tempfile.gettempdir()
OWNER = os.getenv("OWNER", "N/A")


class InputOutputConfig(ConfigurableResource):  # type: ignore[misc] # should be ok in 1.8 version of Dagster
    input_dir: str = Field(
        description="Path to user's files. If not set, it will be defaulted to the temp directory."
    )
    output_dir: str = Field(
        description="Path to the output directory. If not set, it will be defaulted to the temp dir."
    )

    @field_validator("input_dir")
    def input_dir_exists(cls, v: str) -> str:
        if "INPUT_DIR" not in os.environ:
            return TEMP_DIR
        return v

    @field_validator("output_dir")
    def output_dir_exists(cls, v: str) -> str:
        if "OUTPUT_DIR" not in os.environ:
            return TEMP_DIR
        return v

    def get_paths(self) -> dict:
        # main folders
        USER_DATA = self.input_dir
        SYNPHAGE_DATA = self.output_dir

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

        return {
            "USER_DATA": USER_DATA,
            "SYNPHAGE_DATA": SYNPHAGE_DATA,
            "DOWNLOAD_DIR": DOWNLOAD_DIR,
            "GENBANK_DIR": GENBANK_DIR,
            "FILESYSTEM_DIR": FILESYSTEM_DIR,
            "GENE_DIR": GENE_DIR,
            "PROTEIN_DIR": PROTEIN_DIR,
            "TABLES_DIR": TABLES_DIR,
            "SYNTENY_DIR": SYNTENY_DIR,
            "FASTA_N_DIR": FASTA_N_DIR,
            "BLASTN_DB_DIR": BLASTN_DB_DIR,
            "BLASTN_DIR": BLASTN_DIR,
            "FASTA_P_DIR": FASTA_P_DIR,
            "BLASTP_DB_DIR": BLASTP_DB_DIR,
            "BLASTP_DIR": BLASTP_DIR,
        }


class LocalFilesystemIOManager(InputOutputConfig, FilesystemIOManager):  # type: ignore[misc] # should be ok in 1.8 version of Dagster
    def get_io_manager(self) -> FilesystemIOManager:
        fs_dir = InputOutputConfig.get_paths(self)["FILESYSTEM_DIR"]
        return FilesystemIOManager(base_dir=fs_dir)
