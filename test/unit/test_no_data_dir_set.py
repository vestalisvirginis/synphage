import pytest
import os


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_no_data_dir(mock_env_phagy_dir_none):
    assert os.getenv("DATA_DIR") == None
