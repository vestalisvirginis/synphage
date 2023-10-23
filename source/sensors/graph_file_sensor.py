# from dagster import (
#     sensor,
#     RunConfig,
#     RunRequest,
#     SensorDefinition,
#     SkipReason,
#     EnvVar,
# )

# import os
# from pathlib import Path

# from source.assets.status import FileConfig


# def graph_file_sensor(job) -> SensorDefinition:
#     """Returns a sensor that launches the given job when the genbank file has been updated"""

#     @sensor(job=job)
#     def job_sensor():
#         has_files = False
#         MY_DIRECTORY = os.getenv(EnvVar("PHAGY_DIRECTORY"))
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
