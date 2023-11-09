import os

from .ncbi_resource import ncbi_resource
from .parquet_io_manager import LocalParquetIOManager

from dagster import EnvVar, FilesystemIOManager


RESOURCES_LOCAL = {
    "ncbi_connection": ncbi_resource,
    "parquet_io_manager": LocalParquetIOManager(base_dir=EnvVar("PHAGY_DIRECTORY")),
    "io_manager": FilesystemIOManager(
        base_dir="/".join([os.getenv("PHAGY_DIRECTORY"), "fs"])
    ),
}
