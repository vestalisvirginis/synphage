# import pytest
# import os
# import tempfile

# from dagster import build_resources, FilesystemIOManager
# from pydantic import ValidationError
# from pathlib import Path

# from synphage.resources.local_resource import (
#     InputOutputConfig,
#     LocalFilesystemIOManager,
# )


# def test_input_output_config_class():
#     assert callable(InputOutputConfig)
#     configuration = InputOutputConfig(
#         input_dir="input_folder",
#         output_dir="output_folder",
#     )
#     assert hasattr(configuration, "input_dir")
#     assert hasattr(configuration, "output_dir")


# def test_input_output_config_pos():
#     configuration = InputOutputConfig(
#         input_dir="/input_folder", output_dir="/output_folder"
#     )
#     assert configuration.input_dir == "/input_folder"
#     assert configuration.output_dir == "/output_folder"


# def test_input_output_config_missing_values():
#     with pytest.raises(
#         ValidationError,
#         match="2 validation errors for InputOutputConfig",
#     ):
#         InputOutputConfig()


# def test_get_paths():
#     configuration = InputOutputConfig(
#         input_dir="/input_folder", output_dir="/output_folder"
#     )
#     paths = configuration.get_paths()
#     assert isinstance(paths, dict)
#     assert len(paths) == 15
#     keys = [
#         "USER_DATA",
#         "SYNPHAGE_DATA",
#         "DOWNLOAD_DIR",
#         "GENBANK_DIR",
#         "FILESYSTEM_DIR",
#         "GENE_DIR",
#         "PROTEIN_DIR",
#         "TABLES_DIR",
#         "SYNTENY_DIR",
#         "FASTA_N_DIR",
#         "BLASTN_DB_DIR",
#         "BLASTN_DIR",
#         "FASTA_P_DIR",
#         "BLASTP_DB_DIR",
#         "BLASTP_DIR",
#     ]
#     for k in paths.keys():
#         assert k in keys


# def test_input_validator(mock_env_input_validator):
#     TEMP_DIR = tempfile.gettempdir()
#     with build_resources(
#         resources={
#             "path_config": InputOutputConfig(
#                 input_dir=str(os.getenv("INPUT_DIR")),
#                 output_dir=str(os.getenv("OUTPUT_DIR")),
#             )
#         }
#     ) as resources:  # , "from_val": "bar"}) as resources:
#         assert resources.path_config == InputOutputConfig(
#             input_dir=TEMP_DIR, output_dir="/output_folder"
#         )
#         assert resources.path_config.get_paths()["USER_DATA"] == TEMP_DIR
#         assert resources.path_config.get_paths()["SYNPHAGE_DATA"] == "/output_folder"
#         assert resources.path_config.get_paths()["DOWNLOAD_DIR"] == str(
#             Path("/output_folder") / "download"
#         )


# def test_output_validator(mock_env_output_validator):
#     TEMP_DIR = tempfile.gettempdir()
#     with build_resources(
#         resources={
#             "path_config": InputOutputConfig(
#                 input_dir=str(os.getenv("INPUT_DIR")),
#                 output_dir=str(os.getenv("OUTPUT_DIR")),
#             )
#         }
#     ) as resources:
#         assert resources.path_config == InputOutputConfig(
#             input_dir="/input_folder", output_dir=TEMP_DIR
#         )
#         assert resources.path_config.get_paths()["USER_DATA"] == "/input_folder"
#         assert resources.path_config.get_paths()["SYNPHAGE_DATA"] == TEMP_DIR
#         assert resources.path_config.get_paths()["DOWNLOAD_DIR"] == str(
#             Path(TEMP_DIR) / "download"
#         )


# def test_local_iomanager_class():
#     assert callable(LocalFilesystemIOManager)
#     configuration = LocalFilesystemIOManager(
#         input_dir="input_folder",
#         output_dir="output_folder",
#     )
#     assert hasattr(configuration, "input_dir")
#     assert hasattr(configuration, "output_dir")


# def test_get_io_manager():
#     configuration = LocalFilesystemIOManager(
#         input_dir="/input_folder", output_dir="/output_folder"
#     )
#     io_manager = configuration.get_io_manager()
#     assert isinstance(io_manager, FilesystemIOManager)
#     assert getattr(io_manager, "base_dir") == str(Path(configuration.output_dir) / "fs")
