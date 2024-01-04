# from dagster import ConfigurableIOManager


# class LocalParquetIOManager(ConfigurableIOManager):
#     base_dir: str

# def _get_path(self, context):
#     return "/".join(
#         [
#             context.resource_config["base_dir"],
#             context.metadata["tables"],
#             context.metadata["name"],
#         ]
#     )
#     # return "/".join([context.resource_config["base_dir"], context.op_config["output_folder"], context.op_config["name"]]) AttributeError: 'OutputContext' object has no attribute 'op_config'

# def handle_output(self, context, obj):
#     if context.metadata["parquet_managment"] == "overwrite":
#         obj.coalesce(1).write.mode("overwrite").parquet(self._get_path(context))
#     else:
#         obj.coalesce(1).write.mode("append").parquet(self._get_path(context))

# def load_input(self, context):  # context: InputContext
#     spark = SparkSession.builder.getOrCreate()
#     return spark.read.parquet(self._get_path(context.upstream_output))
