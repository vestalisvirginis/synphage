from dagster import (
    sensor,
    RunConfig,
    RunRequest,
    SensorDefinition,
    SkipReason,
    EnvVar,
)

import os
from pathlib import Path

# from source.assets.status import FileConfig


# def genbank_file_update_sensor(job) -> SensorDefinition:
#     """Returns a sensor that launches the given job when the genbank file has been updated"""

#     @sensor(job=job)
#     def job_sensor(context):
#         MY_DIRECTORY = "data_folder/experimenting/genbank"
#         has_files = False
#         last_mtime = float(context.cursor) if context.cursor else 0

#         max_mtime = last_mtime
#         for filename in os.listdir(MY_DIRECTORY):
#             filepath = os.path.join(MY_DIRECTORY, filename)
#             if os.path.isfile(filepath):
#                 fstats = os.stat(filepath)
#                 file_mtime = fstats.st_mtime
#                 if file_mtime <= last_mtime:
#                     continue

#                 # the run key should include mtime if we want to kick off new runs based on file modifications
#                 run_key = f"{Path(filename).stem}:{file_mtime}"
#                 run_config = RunConfig(
#                     ops={
#                         "process_asset": {
#                             "ops": {"process_file": FileConfig(filename=filename)}
#                         },
#                     }
#                 )
#                 yield RunRequest(run_key=run_key, run_config=run_config)
#                 max_mtime = max(max_mtime, file_mtime)
#                 has_files = True
#         if not has_files:
#             yield SkipReason(f"No new files found in {MY_DIRECTORY}.")
#         context.update_cursor(str(max_mtime))

#     return job_sensor


# -------------------  WORKING SENSOR
# def genbank_file_update_sensor(job) -> SensorDefinition:
#     """Returns a sensor that launches the given job when the genbank file has been updated"""

#     @sensor(job=job)
#     def job_sensor():
#         has_files = False
#         MY_DIRECTORY = "/".join([os.getenv(EnvVar("PHAGY_DIRECTORY")), "genbank"])
#         files = os.listdir(MY_DIRECTORY)
#         for filename in files:
#             yield RunRequest(
#                 run_key=Path(filename).stem,
#                 run_config=RunConfig(
#                     ops={
#                         "process_asset": {
#                             "ops": {"process_file": FileConfig(filename=filename)}
#                         }
#                     }
#                 ),
#             )
#             has_files = True
#         if not has_files:
#             yield SkipReason(f"No new files found in {MY_DIRECTORY}.")

#     return job_sensor


# __________________ END OF WORKING SENSOR


# @sensor(name=f"{job.name}_on_genbank_files_updated", job=job)
# def my_directory_sensor_cursor(context):
#     MY_DIRECTORY = './data_folder/experimenting/genbank/'
#     has_files = False
#     last_mtime = float(context.cursor) if context.cursor else 0

#     max_mtime = last_mtimesqlite3.OperationalError: database is lock
#     for filename in os.listdir(MY_DIRECTORY):
#         filepath = os.path.join(MY_DIRECTORY, filename)
#         if os.path.isfile(filepath):
#             fstats = os.stat(filepath)
#             file_mtime = fstats.st_mtime
#             if file_mtime <= last_mtime:
#                 continue

#             # the run key should include mtime if we want to kick off new runs based on file modifications
#             run_key = f"{filename}:{file_mtime}"
#             run_config = {"ops": {"process_file": {"config": {"filename": filename}}}}
#             yield RunRequest(run_key=run_key, run_config=run_config)
#             max_mtime = max(max_mtime, file_mtime)
#             has_files = True
#     if not has_files:
#         yield SkipReason(f"No new files found in {MY_DIRECTORY}.")
#     context.update_cursor(str(max_mtime))
#     return my_directory_sensor_cursor

# def hn_tables_updated_sensor(context):
#     cursor_dict = json.loads(context.cursor) if context.cursor else {}
#     comments_cursor = cursor_dict.get("comments")
#     stories_cursor = cursor_dict.get("stories")

#     comments_event_records = context.instance.get_event_records(
#         EventRecordsFilter(
#             event_type=DagsterEventType.ASSET_MATERIALIZATION,
#             asset_key=AssetKey(["snowflake", "core", "comments"]),
#             after_cursor=comments_cursor,
#         ),
#         ascending=False,
#         limit=1,
#     )
#     stories_event_records = context.instance.get_event_records(
#         EventRecordsFilter(
#             event_type=DagsterEventType.ASSET_MATERIALIZATION,
#             asset_key=AssetKey(["snowflake", "core", "stories"]),
#             after_cursor=stories_cursor,
#         ),
#         ascending=False,
#         limit=1,
#     )

#     if not comments_event_records or not stories_event_records:
#         return

#     # make sure we only generate events if both table_a and table_b have been materialized since
#     # the last evaluation.
#     yield RunRequest(run_key=None)

#     # update the sensor cursor by combining the individual event cursors from the two separate
#     # asset event streams
#     context.update_cursor(
#         json.dumps(
#             {
#                 "comments": comments_event_records[0].storage_id,
#                 "stories": stories_event_records[0].storage_id,
#             }
#         )
#     )

# return hn_tables_updated_sensor
