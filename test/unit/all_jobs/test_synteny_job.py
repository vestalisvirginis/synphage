import pytest
from dagster import ExecuteInProcessResult, Definitions

from synphage.jobs import plot


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_plot():
    defs = Definitions(jobs=[plot])
    result = defs.get_job_def("plot").execute_in_process()
    assert isinstance(result, ExecuteInProcessResult)
    assert result.success

    # add result for each node / check how nodes look like for asset job
    # assert isinstance(result.output_for_node('blastn'), PipeConfig)
    # assert isinstance(result.output_for_node('locus'), PipeConfig)
    # assert isinstance(result.output_for_node('load_blastn'), dict)
    # assert isinstance(result.output_for_node('load_locus'), dict)
