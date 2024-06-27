from .ncbi_resource import ncbi_resource
from synphage.synphage_settings import FILESYSTEM_DIR

from dagster import FilesystemIOManager


RESOURCES_LOCAL = {
    "ncbi_connection": ncbi_resource,
    "io_manager": FilesystemIOManager(base_dir=FILESYSTEM_DIR),
}
