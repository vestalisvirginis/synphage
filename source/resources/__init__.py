# from dagster_pyspark import pyspark_resource

from .ncbi_resource import ncbi_resource

# from .parquet_io_manager import local_partitioned_parquet_io_manager
from .parquet_io_manager import LocalParquetIOManager

from dagster import EnvVar

# configured_pyspark = pyspark_resource.configured()


RESOURCES_LOCAL = {
    "ncbi_connection": ncbi_resource,
    "parquet_io_manager": LocalParquetIOManager(base_dir=EnvVar("PHAGY_DIRECTORY")),
    # "parquet_io_manager": local_partitioned_parquet_io_manager,
    # "pyspark": configured_pyspark,
}
