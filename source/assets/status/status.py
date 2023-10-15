from dagster import (
    asset,
    op,
    multi_asset,
    AssetOut,
    graph_asset,
    Output,
    define_asset_job,
    AssetSelection,
    job,
    Definitions,
    sensor,
    RunConfig,
    RunRequest,
    Config,
)
import os
from pathlib import Path
import pickle


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
