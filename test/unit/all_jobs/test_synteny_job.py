from dagster import ExecuteInProcessResult, Definitions

from synphage.jobs import synteny_job


def test_synteny_job():
    defs = Definitions(jobs=[synteny_job])
    result = defs.get_job_def("synteny_job").execute_in_process()
    assert isinstance(result, ExecuteInProcessResult)
    assert result.success

    # add result for each node / check how nodes look like for asset job
    # assert isinstance(result.output_for_node('blastn'), PipeConfig)
    # assert isinstance(result.output_for_node('locus'), PipeConfig)
    # assert isinstance(result.output_for_node('load_blastn'), dict)
    # assert isinstance(result.output_for_node('load_locus'), dict)
