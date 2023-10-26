from dagster import Definitions

from .assets import (
    ncbi_connect_assets,
    blaster_assets,
    viewer_assets,
    status_assets,
)

from .jobs import blasting_job, transform, synteny_job

from .resources import RESOURCES_LOCAL


all_assets = [
    *ncbi_connect_assets,
    *blaster_assets,
    *viewer_assets,
    *status_assets,
]


resources_by_deployment_name = {
    "local": RESOURCES_LOCAL,
}


defs = Definitions(
    assets=all_assets,
    resources=resources_by_deployment_name["local"],
    jobs=[blasting_job, transform, synteny_job],
)
