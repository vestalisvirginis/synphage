import pytest
from dagster import ExecuteInProcessResult, Definitions, load_assets_from_modules

from synphage.jobs import blast

from synphage.assets.blaster import n_blaster_old
from synphage.assets.status import gb_file_status


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_blast():
    all_assets = load_assets_from_modules([gb_file_status, n_blaster_old])
    defs = Definitions(assets=all_assets, jobs=[blast])
    result = defs.get_job_def("blast").execute_in_process()
    assert isinstance(result, ExecuteInProcessResult)
    assert result.success

    # add result for each node / check how nodes look like for asset job
