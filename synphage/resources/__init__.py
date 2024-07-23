from .local_resource import InputOutputConfig, LocalFilesystemIOManager
from .ncbi_resource import ncbi_resource

import os
import warnings
from dagster import PipesSubprocessClient, ExperimentalWarning

warnings.filterwarnings("ignore", category=ExperimentalWarning)


init_local_io_manager = LocalFilesystemIOManager(
    input_dir=str(os.getenv("INPUT_DIR")), output_dir=str(os.getenv("OUTPUT_DIR"))
)


RESOURCES_LOCAL = {
    "local_resource": InputOutputConfig(
        input_dir=str(os.getenv("INPUT_DIR")), output_dir=str(os.getenv("OUTPUT_DIR"))
    ),
    "ncbi_connection": ncbi_resource,
    "io_manager": init_local_io_manager.get_io_manager(),
    "pipes_subprocess_client": PipesSubprocessClient(),
}
