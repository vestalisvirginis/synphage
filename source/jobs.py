from dagster import AssetSelection, define_asset_job, ScheduleDefinition

from .sensors import genbank_file_update_sensor


asset_job_sensor = genbank_file_update_sensor(
    define_asset_job(
        name="load_job",
        selection=AssetSelection.groups("Status")
        | (
            AssetSelection.groups("Blaster")
            & AssetSelection.keys("process_asset").downstream()
        )
        | AssetSelection.keys("extract_locus_tag_gene"),
        tags={"dagster/priority": "0"},
    )
)


# Job triggering the json files parsing
parsing_job = define_asset_job(
    name="parsing_job",
    selection=AssetSelection.keys("parse_blastn").required_multi_asset_neighbors(),
)

parsing_schedule = ScheduleDefinition(
    job=parsing_job,
    cron_schedule="*/1 * * * *",  # every minute
    tags={"dagster/priority": "1"},
)


# Job triggering the update of the last tables based on Locus_and_gene and blastn dataframes
uniq_job = define_asset_job(
    name="uniq_job",
    selection=AssetSelection.keys("gene_presence_table"),
)

uniq_schedule = ScheduleDefinition(
    job=uniq_job,
    cron_schedule="*/5 * * * *",  # every hour minute???
    tags={"dagster/priority": "2"},
)
