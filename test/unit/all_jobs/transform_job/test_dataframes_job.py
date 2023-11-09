import pytest

from pydantic import ValidationError
from dagster import RunConfig

from synphage.jobs import (
    PipeConfig,
    # setup,
    load,
    parse_blastn,
    parse_locus,
    append,
    gene_presence,
    transform,
)


def test_pipeconfig_pos():
    assert callable(PipeConfig)
    configuration = PipeConfig(
        source="a",
        target="b",
        table_dir="c",
        file="d.parquet",
    )
    assert hasattr(configuration, "source")
    assert hasattr(configuration, "target")
    assert hasattr(configuration, "table_dir")
    assert hasattr(configuration, "file")


def test_pipeconfig_neg():
    with pytest.raises(ValidationError, match="1 validation error for PipeConfig"):
        PipeConfig()


@pytest.mark.parametrize(
    "config, result",
    [
        [
            PipeConfig(source="a"),
            {"source": "a", "target": None, "table_dir": None, "file": "out.parquet"},
        ],
        [
            PipeConfig(source="a", target="b"),
            {"source": "a", "target": "b", "table_dir": None, "file": "out.parquet"},
        ],
        [
            PipeConfig(source="a", target="b", table_dir="c"),
            {"source": "a", "target": "b", "table_dir": "c", "file": "out.parquet"},
        ],
        [
            PipeConfig(source="a", target="b", table_dir="c", file="d.parquet"),
            {"source": "a", "target": "b", "table_dir": "c", "file": "d.parquet"},
        ],
    ],
    ids=["source_value", "target_value", "table_dir", "file"],
)
def test_pipeconfig_param(config, result):
    configuration = config
    assert configuration.dict() == result


# def test_setup():
#     test_config = PipeConfig(
#         source='a',
#     )
#     rs = setup(test_config)
#     assert rs.dict() == {'source': 'a', 'target': None, 'table_dir': None, 'file': 'out.parquet'}


@pytest.mark.skip(reason="no way of currently testing this")
def test_load():
    config = PipeConfig(source="test/fixtures/synthetic_data/genbank")
    load(config)


@pytest.mark.skip(reason="no way of currently testing this")
def test_parse_blastn():
    pass


@pytest.mark.skip(reason="no way of currently testing this")
def test_parse_locus():
    pass


@pytest.mark.skip(reason="no way of currently testing this")
def test_append():
    pass


@pytest.mark.skip(reason="no way of currently testing this")
def test_gene_presence():
    pass


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
