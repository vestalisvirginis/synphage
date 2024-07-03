import os
import tempfile

from pathlib import Path
from dagster import asset, Definitions, ConfigurableResource
from pydantic import Field, validator


TEMP_DIR = tempfile.gettempdir()


class InputOutputConfig(ConfigurableResource):
    input_dir: str = Field(
        description="Path to user's files. If not set, it will be defaulted to the temp directory."
    )
    output_dir: str = Field(
        description="Path to the output directory. If not set, it will be defaulted to the temp dir."
    )

    @validator("input_dir")
    def data_dir_exists(cls, v):
        if "DATA_DIR" not in os.environ:
            return TEMP_DIR
        return v

    @validator("output_dir")
    def output_dir_exists(cls, v):
        if "OUTPUT_DIR" not in os.environ:
            return TEMP_DIR
        return v

    def path_to_main_folders(self) -> dict:
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


# @resource(
#     description="Setup unique path config for the pipeline",
#     #compute_kind="Config",
#     #metadata={"owner": "Virginie Grosboillot"},
# )
# def setup_input_output_config(config: InputOutputConfig) -> InputOutputConfig:
#     """Input and Output directories"""
#     return config


@asset(description="test resource directory)")
def get_directory(context, path_resource: InputOutputConfig) -> str:
    paths = path_resource.path_to_main_folders()
    user = paths["USER_DATA"]
    synphage = paths["SYNPHAGE_DATA"]
    download = paths["DOWNLOAD_DIR"]
    context.log.info(f"User: {user}, synphage: {synphage}, download: {download}")
    return user


defs = Definitions(
    assets=[get_directory],
    resources={
        "path_resource": InputOutputConfig(
            input_dir=str(os.getenv("D_DIR")), output_dir=str(os.getenv("OUTPUT_DIR"))
        )
    },
)

# TEMP_DIR = tempfile.gettempdir()
# USER_DATA = str((os.getenv("DATA_DIR"), TEMP_DIR))
# SYNPHAGE_DATA = str((os.getenv("OUTPUT_DIR"), TEMP_DIR))

# # main folders
# # Path to folder containing the download gb files from the ncbi database
# DOWNLOAD_DIR = str(Path(SYNPHAGE_DATA) / "download")
# # Path to folder containing the genbank files
# GENBANK_DIR = str(Path(SYNPHAGE_DATA) / "genbank")
# # Path to folder containing the file system files
# FILESYSTEM_DIR = str(Path(SYNPHAGE_DATA) / "fs")
# # Path to folder containing the blastn related folders
# GENE_DIR = str(Path(SYNPHAGE_DATA) / "gene_identity")
# # Path to folder containing the blastp related folders
# PROTEIN_DIR = str(Path(SYNPHAGE_DATA) / "protein_identity")
# # Path to folder containing the data tables
# TABLES_DIR = str(Path(SYNPHAGE_DATA) / "tables")
# # Path to folder containing the plot files
# SYNTENY_DIR = str(Path(SYNPHAGE_DATA) / "synteny")

# # blastn related folders
# # Path to folder containing the fasta files
# FASTA_N_DIR = str(Path(GENE_DIR) / "fasta_n")
# # Path to folder containing the database to perform the blastn
# BLASTN_DB_DIR = str(Path(GENE_DIR) / "blastn_database")
# # Path to folder containing the blastn json files
# BLASTN_DIR = str(Path(GENE_DIR) / "blastn")

# # blastp related folders
# # Path to folder containing the fasta files
# FASTA_P_DIR = str(Path(PROTEIN_DIR) / "fasta_p")
# # Path to folder containing the database to perform the blastp
# BLASTP_DB_DIR = str(Path(PROTEIN_DIR) / "blastp_database")
# # Path to folder containing the blastp json files
# BLASTP_DIR = str(Path(PROTEIN_DIR) / "blastp")
