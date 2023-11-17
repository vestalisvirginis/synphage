from synphage.jobs import setup_config, PipeConfig


def test_setup_config():
    result = setup_config(PipeConfig(source="a", target='b', table_dir='c', file='d.parquet'))
    assert isinstance(result, PipeConfig)
    assert result == PipeConfig(
        source="a", target='b', table_dir='c', file='d.parquet'
    )
