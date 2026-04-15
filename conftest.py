import pytest

from unittest.mock import Mock, MagicMock

from synphage.resources.ncbi_resource import NCBIConnection


@pytest.fixture
def mock_ncbi_connection():
    """
    Mock NCBI connection: simulates Entrez esearch response
    """

    def _create_mock(read_return_value=None):
        if read_return_value is None:
            read_return_value = {
                "Count": "2",
                "QueryTranslation": "Myoalterovirus[All Fields]",
            }

        mock_conn = Mock()
        mock_handle = MagicMock()
        mock_handle.close = Mock()

        mock_conn.esearch.return_value = mock_handle  ## mock esearch output
        mock_conn.read.return_value = read_return_value  ## mock read output

        return mock_conn

    return _create_mock


@pytest.fixture
def mock_ncbi_resource(mock_ncbi_connection):
    """
    Create a mock NCBIConnection resource
    """
    mock_ncbi = Mock(spec=NCBIConnection)  ## mock NCBIConnection resource
    mock_ncbi.conn = mock_ncbi_connection()  # Call the factory to get the actual mock
    return mock_ncbi


@pytest.fixture
def mock_env_input_validator(monkeypatch):
    monkeypatch.delenv("INPUT_DIR")
    monkeypatch.setenv("OUTPUT_DIR", "/output_folder")


@pytest.fixture
def mock_env_output_validator(monkeypatch):
    monkeypatch.setenv("INPUT_DIR", "/input_folder")
    monkeypatch.delenv("OUTPUT_DIR")


@pytest.fixture
def mock_env_ncbi_connect(monkeypatch):
    monkeypatch.setenv("EMAIL", "name@domain.com")
    monkeypatch.setenv("API_KEY", "jhd6hdz778ahjeahj8889")


@pytest.fixture
def mock_env_ncbi_download_pos(monkeypatch):
    monkeypatch.setenv(
        "OUTPUT_DIR", "test/fixtures/assets_testing_folder/ncbi_download/positive"
    )


@pytest.fixture
def mock_env_ncbi_download_neg(monkeypatch):
    monkeypatch.setenv(
        "OUTPUT_DIR", "test/fixtures/assets_testing_folder/ncbi_download/negative"
    )


@pytest.fixture
def mock_env_ncbi_fetch(monkeypatch):
    monkeypatch.setenv(
        "OUTPUT_DIR", "test/fixtures/assets_testing_folder/ncbi_download/fetch"
    )


@pytest.fixture
def mock_env_download_to_genbank(monkeypatch):
    monkeypatch.setenv(
        "OUTPUT_DIR", "test/fixtures/assets_testing_folder/download_to_genbank"
    )


@pytest.fixture
def mock_env_download_to_genbank_with_history(monkeypatch):
    monkeypatch.setenv(
        "OUTPUT_DIR",
        "test/fixtures/assets_testing_folder/download_to_genbank_with_history",
    )


@pytest.fixture
def mock_env_users_to_genbank(monkeypatch):
    monkeypatch.setenv("INPUT_DIR", "test/fixtures/user_data_transfer/user_data")
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/user_data_transfer/data")


@pytest.fixture
def mock_env_users_to_genbank_with_history(monkeypatch):
    monkeypatch.setenv("INPUT_DIR", "test/fixtures/user_data_transfer/user_data")
    monkeypatch.setenv(
        "OUTPUT_DIR", "test/fixtures/user_data_transfer/data_with_history"
    )


@pytest.fixture
def mock_env_users_to_genbank_empty_dir(monkeypatch):
    monkeypatch.setenv("INPUT_DIR", "test/fixtures/user_data_transfer")
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/user_data_transfer/data")


@pytest.fixture
def mock_env_phagy_dir_blasting(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "test/fixtures/assets_testing_folder/blasting")


@pytest.fixture
def mock_env_phagy_dir_blasting_with_history(monkeypatch):
    monkeypatch.setenv(
        "OUTPUT_DIR", "test/fixtures/assets_testing_folder/blasting_with_history"
    )


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
    monkeypatch.setenv(
        "OUTPUT_DIR", "test/fixtures/assets_testing_folder/synteny_no_csv"
    )


@pytest.fixture
def mock_env_phagy_dir_none(monkeypatch):
    monkeypatch.delenv("OUTPUT_DIR", raising=False)
