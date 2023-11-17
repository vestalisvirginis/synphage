import pytest

from pydantic import ValidationError
from dagster import RunConfig

from synphage.jobs import (
    PipeConfig,
    transform,
)


@pytest.mark.skip(reason="no way of currently testing this")
def test_transform():
    test_config = RunConfig(
        ops={
            "blastn": PipeConfig(
                source="test/fixtures/synthetic_data/blast_results",
                target="test/fixtures/dataframes_job/blastn_parsing",
                table_dir="test/fixtures/dataframes_job",
                file="blastn_summary.parquet",
            ),
            "locus": PipeConfig(
                source="test/fixtures/synthetic_data/genbank",
                target="test/fixtures/dataframes_job/locus_parsing",
                table_dir="test/fixtures/dataframes_job",
                file="locus_and_gene.parquet",
            ),
        }
    )
    results = transform.execute_in_process(test_config)

    # return type is ExecuteInProcessResult
    assert isinstance(result, ExecuteInProcessResult)
    assert result.success


# def test_job():
#     result = do_math_job.execute_in_process()

#     # return type is ExecuteInProcessResult
#     assert isinstance(result, ExecuteInProcessResult)
#     assert result.success
#     # inspect individual op result
#     assert result.output_for_node("add_one") == 2
#     assert result.output_for_node("add_two") == 3
#     assert result.output_for_node("subtract") == -1
