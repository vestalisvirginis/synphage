from dagster import ExecuteInProcessResult, Definitions

from synphage.jobs import download


def test_download():
    defs = Definitions(jobs=[download])
    result = defs.get_job_def("download").execute_in_process()
    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
