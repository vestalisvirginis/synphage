# import os

# from dagster import IOManager, io_manager
# from pyspark.sql import SparkSession


# class LocalParquetIOManager(IOManager):
#     def _get_path(self, context):
#         return os.path.join(context.run_id, context.step_key, context.name)

#     def handle_output(self, context, obj):
#         obj.write.parquet(self._get_path(context))

#     def load_input(self, context):
#         spark = SparkSession.builder.getOrCreate()
#         return spark.read.parquet(self._get_path(context.upstream_output))


# @io_manager
# def local_partitioned_parquet_io_manager(init_context):
#     return LocalParquetIOManager()
