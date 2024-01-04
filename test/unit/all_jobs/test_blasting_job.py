from dagster import ExecuteInProcessResult, Definitions, load_assets_from_modules

from synphage.jobs import blasting_job

from synphage.assets.blaster import n_blaster
from synphage.assets.status import status


def test_blasting_job():
    all_assets = load_assets_from_modules([status, n_blaster])
    defs = Definitions(assets=all_assets, jobs=[blasting_job])
    result = defs.get_job_def("blasting_job").execute_in_process()
    assert isinstance(result, ExecuteInProcessResult)
    assert result.success

    # add result for each node / check how nodes look like for asset job
