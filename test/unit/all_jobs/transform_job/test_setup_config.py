from synphage.jobs import setup_config, PipeConfig

def test_setup_config():
    result = setup_config(PipeConfig(source='a'))
    assert result == {"source": "a", "target": None, "table_dir": None, "file": "out.parquet"}