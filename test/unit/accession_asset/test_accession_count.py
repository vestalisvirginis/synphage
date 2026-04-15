import pytest

from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.ncbi_connect.accession import accession_count, QueryConfig


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


def test_accession_count_asset_with_search_key(
    mock_ncbi_resource, mock_ncbi_connection
):

    MOCK_RESPONSE = {
        "Count": "10",
        "QueryTranslation": "Bacillus subtilis strain P9_B1[All Fields]",
    }
    mock_ncbi_resource.conn = mock_ncbi_connection(MOCK_RESPONSE)

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
