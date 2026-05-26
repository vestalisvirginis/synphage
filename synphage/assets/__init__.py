from dagster import load_assets_from_package_module
from . import phold

from . import (
    ncbi_connect,
    user_data,
    status,
    blaster,
    viewer,
    structure_module,
)

NCBI_CONNECT = "ncbi_connect"
USERS_INPUT = "users_input"
STATUS = "status"
BLASTER = "blaster"
VIEWER = "viewer"
STRUCTURE_MODULE = "structure_module"
PHOLD = "phold"

ncbi_connect_assets = load_assets_from_package_module(
    package_module=ncbi_connect,
    group_name=NCBI_CONNECT,
)

user_input_assets = load_assets_from_package_module(
    package_module=user_data,
    group_name=USERS_INPUT,
)

status_assets = load_assets_from_package_module(
    package_module=status,
    group_name=STATUS,
)

blaster_assets = load_assets_from_package_module(
    package_module=blaster,
    group_name=BLASTER,
)

viewer_assets = load_assets_from_package_module(
    package_module=viewer,
    group_name=VIEWER,
)

structure_module_assets = load_assets_from_package_module(
    package_module=structure_module,
    group_name=STRUCTURE_MODULE,
)

phold_assets = load_assets_from_package_module(
    package_module=phold,
    group_name=PHOLD,
)
