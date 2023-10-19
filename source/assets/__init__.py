from dagster import load_assets_from_package_module

from . import ncbi_connect, status, blaster, viewer, tables

NCBI_CONNECT = "NCBI_connect"
STATUS = "Status"
BLASTER = "Blaster"
TABLES = "Tables"
VIEWER = "Viewer"

ncbi_connect_assets = load_assets_from_package_module(
    package_module=ncbi_connect,
    group_name=NCBI_CONNECT,
)

status_assets = load_assets_from_package_module(
    package_module=status,
    group_name=STATUS,
)

blaster_assets = load_assets_from_package_module(
    package_module=blaster,
    group_name=BLASTER,
)

tables_assets = load_assets_from_package_module(
    package_module=tables,
    group_name=TABLES,
)

viewer_assets = load_assets_from_package_module(
    package_module=viewer,
    group_name=VIEWER,
)
