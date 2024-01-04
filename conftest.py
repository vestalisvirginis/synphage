import pytest


@pytest.fixture
def mock_env_ncbi_connect(monkeypatch):
    monkeypatch.setenv("EMAIL", "name@domain.com")
    monkeypatch.setenv("API_KEY", "jhd6hdz778ahjeahj8889")

@pytest.fixture
def mock_env_ncbi_download_pos(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "test/fixtures/ncbi_download/positive")

@pytest.fixture
def mock_env_ncbi_download_neg(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "test/fixtures/ncbi_download/negative")

@pytest.fixture
def mock_env_ncbi_count(monkeypatch):
    monkeypatch.setenv("DATABASE", "nuccore")
    monkeypatch.setenv("KEYWORD", "Bacillus subtilis strain P9_B1")

@pytest.fixture
def mock_env_ncbi_fetch(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "test/fixtures/ncbi_download/fetch")

@pytest.fixture
def mock_env_sequence_check(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "test/fixtures/assets_testing_folder/sequence_quality")

@pytest.fixture
def mock_env_sequence_check_with_history(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "test/fixtures/assets_testing_folder/sequence_quality_with_history")

@pytest.fixture
def mock_env_phagy_dir_blasting(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "test/fixtures/assets_testing_folder/blasting")

@pytest.fixture
def mock_env_phagy_dir_blasting_with_history(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "test/fixtures/assets_testing_folder/blasting_with_history")

@pytest.fixture
def mock_env_phagy_dir_transform(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "test/fixtures/assets_testing_folder/transform")
    monkeypatch.setenv("FILE_SYSTEM", "fs")

@pytest.fixture
def mock_env_phagy_dir_transform_step3(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "test/fixtures/assets_testing_folder/transform_3")

@pytest.fixture
def mock_env_phagy_dir_synteny(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "test/fixtures/assets_testing_folder/synteny")
    monkeypatch.setenv("SEQUENCE_FILE", "sequences.csv")
    monkeypatch.delenv("TITLE", raising=False)

@pytest.fixture
def mock_env_phagy_dir_synteny_var(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "test/fixtures/assets_testing_folder/synteny_var")
    monkeypatch.setenv("SEQUENCE_FILE", "sequences.csv")
    monkeypatch.setenv("TITLE", "my_test_title")
    monkeypatch.setenv("GRAPH_FORMAT", "circular")
    monkeypatch.setenv("GRAPH_PAGESIZE", "A0")
    monkeypatch.setenv("GRAPH_START", 1000)
    monkeypatch.setenv("GRAPH_END", 2000)

@pytest.fixture
def mock_env_phagy_dir_none(monkeypatch):
    monkeypatch.delenv("DATA_DIR", raising=False)
    