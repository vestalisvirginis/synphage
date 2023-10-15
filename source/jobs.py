from dagster import AssetSelection, define_asset_job

from .assets import STATUS, status_assets
from .sensors import genbank_file_update_sensor


# blaster_assets_sensor = genbank_file_update_sensor(
#     define_asset_job("blaster_job", selection=AssetSelection.groups(STATUS))
# )

# to test:
asset_job_sensor = genbank_file_update_sensor(
    define_asset_job("load_job", AssetSelection.groups("Status"))
)


# working
# asset_job = define_asset_job("load_job", AssetSelection.groups("STATUS"))
