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


def test_accession_ids(mock_ncbi_resource, mock_ncbi_connection):
    mock_ncbi_resource.conn = mock_ncbi_connection(MOCK_RESPONSE)
    context = build_asset_context(
        resources={"ncbi_connection": mock_ncbi_resource},
    )
    asset_count_input = 2
    asset_config_input = QueryConfig(search_key=TEST_KEY)
    result = accession_ids(context, asset_count_input, asset_config_input)
    assert isinstance(result, dict)
    assert isinstance(result["IdList"], list)
    assert result["IdList"] == ["NZ_CP045811.1", "CP045811.1"]


def test_accession_ids_asset(mock_ncbi_resource, mock_ncbi_connection):
    mock_ncbi_resource.conn = mock_ncbi_connection(MOCK_RESPONSE)

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
