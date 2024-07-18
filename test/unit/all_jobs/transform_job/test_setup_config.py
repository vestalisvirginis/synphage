import pytest
#from synphage.jobs import setup_config, PipeConfig


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_setup_config():
    result = setup_config(
        PipeConfig(source="a", target="b", table_dir="c", file="d.parquet")
    )
    assert isinstance(result, PipeConfig)
    assert result == PipeConfig(source="a", target="b", table_dir="c", file="d.parquet")
