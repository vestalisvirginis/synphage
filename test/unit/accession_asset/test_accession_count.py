import pytest
from unittest.mock import Mock, MagicMock

from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.ncbi_connect.accession import accession_count, QueryConfig
from synphage.resources.ncbi_resource import NCBIConnection


@pytest.fixture
def mock_ncbi_connection():
    """
    Mock NCBI connection: simulates Entrez esearch response
    """
    mock_conn = Mock()
    mock_handle = MagicMock()
    mock_handle.close = Mock()

    mock_conn.esearch.return_value = mock_handle  ## mock esearch output
    mock_conn.read.return_value = {  ## mock read output
        "Count": "2",
        "QueryTranslation": "Myoalterovirus[All Fields]",
    }
    return mock_conn


@pytest.fixture
def mock_ncbi_resource(mock_ncbi_connection):
    """
    Create a mock NCBIConnection resource
    """
    mock_ncbi = Mock(spec=NCBIConnection)  ## mock NCBIConnection resource
    mock_ncbi.conn = mock_ncbi_connection
    return mock_ncbi


def test_accession_count(mock_ncbi_resource):
    context = build_asset_context(resources={"ncbi_connection": mock_ncbi_resource})
    input_asset = QueryConfig()
    result = accession_count(context, input_asset)
    assert isinstance(result, int)
    assert result == 2


def test_accession_count_asset(mock_ncbi_resource):
    @asset(name="setup_query_config")
    def mock_upstream():
        return QueryConfig()

    assets = [accession_count, mock_upstream]
    result = materialize_to_memory(
        assets,
        resources={"ncbi_connection": mock_ncbi_resource},
    )
    assert result.success
    acc_count = result.output_for_node("accession_count")
    assert acc_count == 2
    for k, v in result.asset_materializations_for_node("accession_count")[
        0
    ].metadata.items():
        if k == "search_word(s)":
            assert v.text == "Myoalterovirus"


def test_accession_count_asset_with_search_key(mock_ncbi_connection):

    mock_ncbi_connection.read.return_value = {
        "Count": "10",
        "QueryTranslation": "Bacillus subtilis strain P9_B1[All Fields]",
    }
    mock_ncbi_resource = Mock(spec=NCBIConnection)
    mock_ncbi_resource.conn = mock_ncbi_connection

    @asset(name="setup_query_config")
    def mock_upstream():
        return QueryConfig(search_key="Bacillus subtilis strain P9_B1")

    assets = [accession_count, mock_upstream]
    result = materialize_to_memory(
        assets,
        resources={"ncbi_connection": mock_ncbi_resource},
    )
    assert result.success
    acc_count = result.output_for_node("accession_count")
    assert acc_count == 10
    for k, v in result.asset_materializations_for_node("accession_count")[
        0
    ].metadata.items():
        if k == "search_word(s)":
            assert v.text == "Bacillus subtilis strain P9_B1"
