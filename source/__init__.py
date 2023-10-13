#from dagster import Definitions
from dagster import op, job, Config, sensor, RunRequest, RunConfig, Definitions
import os

from .assets import ncbi_connect_assets, blaster_assets, viewer_assets
from .resources import RESOURCES_LOCAL


###---------------------------------------------
class FileConfig(Config):     
    filename: str
 

@op
def process_file(context, config: FileConfig):
    context.log.info(config.filename)


@job
def log_file_job():
    process_file()


@sensor(job=log_file_job)
def my_directory_sensor_cursor(context):
    MY_DIRECTORY = './data_folder/experimenting/genbank/'
    has_files = False
    last_mtime = float(context.cursor) if context.cursor else 0

    max_mtime = last_mtime
    for filename in os.listdir(MY_DIRECTORY):
        filepath = os.path.join(MY_DIRECTORY, filename)
        if os.path.isfile(filepath):
            fstats = os.stat(filepath)
            file_mtime = fstats.st_mtime
            if file_mtime <= last_mtime:
                continue

            # the run key should include mtime if we want to kick off new runs based on file modifications
            run_key = f"{filename}:{file_mtime}"
            run_config = {"ops": {"process_file": {"config": {"filename": filename}}}}
            yield RunRequest(run_key=run_key, run_config=run_config)
            max_mtime = max(max_mtime, file_mtime)
            has_files = True
    if not has_files:
        yield SkipReason(f"No files found in {MY_DIRECTORY}.")
    context.update_cursor(str(max_mtime))
###--------------------------------------------------------



all_assets = [*ncbi_connect_assets, *blaster_assets, *viewer_assets]


resources_by_deployment_name = {
    "local": RESOURCES_LOCAL,
}

all_sensors = [my_directory_sensor_cursor]

defs = Definitions(
    assets=all_assets,
    resources=resources_by_deployment_name["local"],
    sensors=all_sensors,
)
