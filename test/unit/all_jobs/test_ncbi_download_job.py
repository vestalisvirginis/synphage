import pytest
from dagster import ExecuteInProcessResult, Definitions

from synphage.jobs import download


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_download():
    defs = Definitions(jobs=[download])
    result = defs.get_job_def("download").execute_in_process()
    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
