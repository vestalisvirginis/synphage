from dagster import AssetSelection, define_asset_job

from .sensors import genbank_file_update_sensor


asset_job_sensor = genbank_file_update_sensor(
    define_asset_job(
        "load_job",
        AssetSelection.groups("Status")
        | (
            AssetSelection.groups("Blaster")
            & AssetSelection.keys("process_asset").downstream()
        ),
    )
)
