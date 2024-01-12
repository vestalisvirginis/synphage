from dagster import ExecuteInProcessResult, Definitions

from synphage.jobs import ncbi_download_job


def test_ncbi_download_job():
    defs = Definitions(jobs=[ncbi_download_job])
    result = defs.get_job_def("ncbi_download_job").execute_in_process()
    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
