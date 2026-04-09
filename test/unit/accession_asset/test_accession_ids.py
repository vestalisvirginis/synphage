import os
import pytest
from unittest.mock import Mock, MagicMock

from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.ncbi_connect.accession import accession_ids, QueryConfig
from synphage.resources.ncbi_resource import NCBIConnection

TEST_KEY = "Bacillus subtilis strain P9_B1"

MOCK_RESPONSE = {
    "IdList": ["NZ_CP045811.1", "CP045811.1"],
    "Count": "2",
    "RetMax": "2",
    "RetStart": "0",
    "QueryKey": "1",
    "WebEnv": "MCID_65957a742f85c1163859e8eb",
}


@pytest.fixture
def mock_ncbi_connection():
    """
    Mock NCBI connection to avoid real API calls.
    Returns a Mock object that simulates Entrez esearch response.
    """
    mock_conn = Mock()

    # Mock the esearch method to return a file-like object
    mock_handle = MagicMock()
    mock_handle.close = Mock()
    mock_conn.esearch.return_value = mock_handle
    mock_conn.read.return_value = MOCK_RESPONSE

    return mock_conn


@pytest.fixture
def mock_ncbi_resource(mock_ncbi_connection):
    """
    Create a mock NCBIConnection resource.
    """
    mock_ncbi = Mock(spec=NCBIConnection)
    mock_ncbi.conn = mock_ncbi_connection
    return mock_ncbi


def test_accession_ids(mock_ncbi_resource):
    """Test accession_ids asset with mocked NCBI connection"""
    context = build_asset_context(
        resources={"ncbi_connection": mock_ncbi_resource},
    )
    asset_count_input = 2
    asset_config_input = QueryConfig(search_key=TEST_KEY)
    result = accession_ids(context, asset_count_input, asset_config_input)
    assert isinstance(result, dict)
    assert isinstance(result["IdList"], list)
    assert result["IdList"] == ["NZ_CP045811.1", "CP045811.1"]


def test_accession_ids_asset(mock_ncbi_resource):
    """Test accession_ids asset materialization with mocked NCBI connection"""

    @asset(name="accession_count")
    def mock_count_upstream():
        return 2

    @asset(name="setup_query_config")
    def mock_config_upstream():
        return QueryConfig(search_key=TEST_KEY)

    assets = [accession_ids, mock_count_upstream, mock_config_upstream]
    result = materialize_to_memory(
        assets,
        resources={"ncbi_connection": mock_ncbi_resource},
    )
    assert result.success
    result_dict = result.output_for_node("accession_ids")
    assert result_dict["IdList"] == ["NZ_CP045811.1", "CP045811.1"]


def test_accession_ids_empty_result(mock_ncbi_connection):
    """
    Test accession_ids when search returns no results.
    Added to ensure edge case where no sequences match the search criteria is handled.
    """
    mock_ncbi_connection.read.return_value = {
        "IdList": [],
        "Count": "0",
        "RetMax": "0",
        "RetStart": "0",
        "QueryKey": "1",
        "WebEnv": "MCID_test",
    }
    mock_ncbi_resource = Mock(spec=NCBIConnection)
    mock_ncbi_resource.conn = mock_ncbi_connection

    context = build_asset_context(
        resources={"ncbi_connection": mock_ncbi_resource},
    )
    asset_count_input = 0
    asset_config_input = QueryConfig(search_key="NoResultsKeyword")
    result = accession_ids(context, asset_count_input, asset_config_input)
    assert isinstance(result, dict)
    assert isinstance(result["IdList"], list)
    assert result["IdList"] == []
    assert len(result["IdList"]) == 0
