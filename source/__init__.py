from dagster import Definitions

from .assets import (
    ncbi_connect_assets,
    blaster_assets,
    viewer_assets,
    status_assets,
    tables_assets,
)
from .jobs import asset_job_sensor, parsing_schedule, uniq_schedule
from .resources import RESOURCES_LOCAL


all_assets = [
    *ncbi_connect_assets,
    *blaster_assets,
    *viewer_assets,
    *status_assets,
    *tables_assets,
]


resources_by_deployment_name = {
    "local": RESOURCES_LOCAL,
}

all_schedules = [parsing_schedule, uniq_schedule]

defs = Definitions(
    assets=all_assets,
    resources=resources_by_deployment_name["local"],
    sensors=[asset_job_sensor],
    # jobs=[tables_job],
    schedules=all_schedules,
)
