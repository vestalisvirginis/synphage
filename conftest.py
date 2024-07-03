import pytest


@pytest.fixture
def mock_env_input_validator(monkeypatch):
    monkeypatch.delenv('DATA_DIR')
    monkeypatch.setenv('OUTPUT_DIR', '/output_folder')

@pytest.fixture
def mock_env_output_validator(monkeypatch):
    monkeypatch.setenv('DATA_DIR', '/input_folder')
    monkeypatch.delenv('OUTPUT_DIR')

@pytest.fixture
def mock_env_ncbi_connect(monkeypatch):
    monkeypatch.setenv("EMAIL", "name@domain.com")
    monkeypatch.setenv("API_KEY", "jhd6hdz778ahjeahj8889")

@pytest.fixture
def mock_env_ncbi_download_pos(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/assets_testing_folder/ncbi_download/positive")

@pytest.fixture
def mock_env_ncbi_download_neg(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/assets_testing_folder/ncbi_download/negative")

@pytest.fixture
def mock_env_ncbi_fetch(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/assets_testing_folder/ncbi_download/fetch")

@pytest.fixture
def mock_env_sequence_check(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/assets_testing_folder/sequence_quality")

@pytest.fixture
def mock_env_sequence_check_with_history(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/assets_testing_folder/sequence_quality_with_history")

@pytest.fixture
def mock_env_phagy_dir_blasting(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/assets_testing_folder/blasting")

@pytest.fixture
def mock_env_phagy_dir_blasting_with_history(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/assets_testing_folder/blasting_with_history")

@pytest.fixture
def mock_env_phagy_dir_transform(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/assets_testing_folder/transform")

@pytest.fixture
def mock_env_phagy_dir_transform_step3(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/assets_testing_folder/transform_3")

@pytest.fixture
def mock_env_phagy_dir_synteny(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/assets_testing_folder/synteny")

@pytest.fixture
def mock_env_phagy_dir_synteny_no_csv(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/assets_testing_folder/synteny_no_csv")

@pytest.fixture
def mock_env_phagy_dir_none(monkeypatch):
    monkeypatch.delenv("OUTPUT_DIR", raising=False)
    