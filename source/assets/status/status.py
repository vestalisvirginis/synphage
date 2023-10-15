from dagster import (
    asset,
    op,
    graph_asset,
    Config,
)


class FileConfig(Config):
    filename: str


@op
def process_file(context, config: FileConfig):
    """Print file name on console"""
    context.log.info(config.filename)
    return config.filename


@graph_asset()
def process_asset():
    return process_file()


@asset()
def downstream_asset(context, process_asset):
    context.log.info("Downstream processing")
    return "TEST"
