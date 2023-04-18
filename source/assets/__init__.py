from dagster import load_assets_from_package_module

from . import ncbi_connect, blaster

NCBI_CONNECT = "NCBI_connect"
BLASTER = "Blaster"

ncbi_connect_assets = load_assets_from_package_module(
    package_module=ncbi_connect,
    group_name=NCBI_CONNECT,
)

blaster_assets = load_assets_from_package_module(
    package_module=blaster,
    group_name=BLASTER,
)
