from typing import List

from dagster import Config, op, Field, asset, EnvVar


# class FileConfig(Config):
#     filename: str


# @op
# def process_file(context, config: FileConfig):
#     context.log.info(config.filename)


# def _standardise_file_extention(file) -> None:
#     """Change file extension when '.gbk' for '.gb'"""
#     path = Path(file)
#     if path.suffix == '.gbk':
#         return path.rename(path.with_suffix('.gb'))


# sqc_folder_config = {
#     "genbank_dir": Field(
#         str,
#         description="Path to folder containing the genbank files",
#         default_value="genbank",
#     ),
#     "fasta_dir": Field(
#         str,
#         description="Path to folder containing the fasta sequence files",
#         default_value="gene_identity/fasta",
#     ),
# }


# @asset(
#     config_schema={**sqc_folder_config},
#     description="""List the sequences available in the genbank folder and return a list""",
#     compute_kind="Python",
#     io_manager_key="io_manager",
#     metadata={"owner": "Virginie Grosboillot"},
# )
# def list_genbank_files(context) -> List[str]:
#     files = list(
#         map(
#             lambda x: Path(x).stem,
#             os.listdir(
#                 "/".join(
#                     [
#                         os.getenv(EnvVar("PHAGY_DIRECTORY")),
#                         context.op_config["genbank_dir"],
#                     ]
#                 )
#             ),
#         )
#     )

#     time = datetime.now()
#     context.add_output_metadata(
#         metadata={
#             "text_metadata": f"List of genbank files {time.isoformat()} (UTC).",
#             "path": "/".join(
#                 [
#                     EnvVar("PHAGY_DIRECTORY"),
#                     context.op_config["genbank_dir"],
#                 ]
#             ),
#             "num_files": len(files),
#             "preview": files,
#         }
#     )

#     return files
