from dagster import load_assets_from_package_module

from . import ncbi_connect

NCBI_CONNECT = "NCBI_connect"

ncbi_connect_assets = load_assets_from_package_module(
    package_module = ncbi_connect,
    group_name = NCBI_CONNECT,
)