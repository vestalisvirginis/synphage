from dagster import ConfigurableIOManager


class LocalParquetIOManager(ConfigurableIOManager):
    base_dir: str

    def _get_path(self, context):
        return "/".join([context.resource_config["base_dir"], context.metadata["output_folder"], context.metadata["name"]])
        # return "/".join([context.resource_config["base_dir"], context.op_config["output_folder"], context.op_config["name"]]) AttributeError: 'OutputContext' object has no attribute 'op_config'

    def handle_output(self, context, obj):
        obj.write.mode("overwrite").parquet(self._get_path(context))

    def load_input(self, context):
        spark = SparkSession.builder.getOrCreate()
        return spark.read.parquet(self._get_path(context.upstream_output))
 