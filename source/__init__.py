from dagster import op, job, Config, sensor, RunRequest, RunConfig, Definitions, define_asset_job, AssetSelection, graph_asset, asset, SkipReason
import os

from .assets import ncbi_connect_assets, blaster_assets, viewer_assets, status_assets
from .jobs import asset_job_sensor
from .resources import RESOURCES_LOCAL

 

all_assets = [*ncbi_connect_assets, *blaster_assets, *viewer_assets, *status_assets]


resources_by_deployment_name = {
    "local": RESOURCES_LOCAL,
}


defs = Definitions(
    assets=all_assets,
    resources=resources_by_deployment_name["local"],
    sensors=[asset_job_sensor],
)
