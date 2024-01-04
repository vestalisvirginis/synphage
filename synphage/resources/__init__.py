import os
import tempfile
from pathlib import Path

from .ncbi_resource import ncbi_resource

from dagster import EnvVar, FilesystemIOManager


RESOURCES_LOCAL = {
    "ncbi_connection": ncbi_resource,
    "io_manager": FilesystemIOManager(
        base_dir=str(Path(os.getenv(EnvVar("DATA_DIR"), tempfile.gettempdir())) / "fs")
    ),
}
