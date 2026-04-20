from dagster import Definitions

from .assets import (
    ncbi_connect_assets,
    user_input_assets,
    blaster_assets,
    viewer_assets,
    status_assets,
    foldseek_assets,
    phold_assets,
)

from .jobs import (
    get_user_data,
    download,
    validations,
    blastn,
    blastp,
    all_blast,
    plot,
    foldseek,
    phold,
)

from .resources import RESOURCES_LOCAL

all_assets = [
    *ncbi_connect_assets,
    *user_input_assets,
    *blaster_assets,
    *viewer_assets,
    *status_assets,
    *foldseek_assets,
    *phold_assets,
]


resources_by_deployment_name = {
    "local": RESOURCES_LOCAL,
}


defs = Definitions(
    assets=all_assets,
    resources=resources_by_deployment_name["local"],
    jobs=[
        get_user_data,
        download,
        validations,
        blastn,
        blastp,
        all_blast,
        plot,
        foldseek,
        phold,
    ],
)
