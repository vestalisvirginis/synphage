import os

from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.ncbi_connect.accession import accession_count, QueryConfig
from synphage.resources.ncbi_resource import NCBIConnection


def test_accession_count():
    context = build_asset_context(
        resources={
            "ncbi_connection": NCBIConnection(
                email=os.getenv("EMAIL"), api_key=os.getenv("API_KEY")
            )
        }
    )
    input_asset = QueryConfig()
    result = accession_count(context, input_asset)
    assert isinstance(result, int)
    assert result == 2


def test_accession_count_asset():
    @asset(name="setup_query_config")
    def mock_upstream():
        return QueryConfig()

    assets = [accession_count, mock_upstream]
    result = materialize_to_memory(
        assets,
        resources={
            "ncbi_connection": NCBIConnection(
                email=os.getenv("EMAIL"), api_key=os.getenv("API_KEY")
            )
        },
    )
    assert result.success
    acc_count = result.output_for_node("accession_count")
    assert acc_count == 2
    for k, v in result.asset_materializations_for_node("accession_count")[
        0
    ].metadata.items():
        if k == "search_word(s)":
            assert v.text == "Myoalterovirus"


def test_accession_count_asset_with_search_key():
    @asset(name="setup_query_config")
    def mock_upstream():
        return QueryConfig(search_key="Bacillus subtilis strain P9_B1")

    assets = [accession_count, mock_upstream]
    result = materialize_to_memory(
        assets,
        resources={
            "ncbi_connection": NCBIConnection(
                email=os.getenv("EMAIL"), api_key=os.getenv("API_KEY")
            )
        },
    )
    assert result.success
    acc_count = result.output_for_node("accession_count")
    assert acc_count == 2
    for k, v in result.asset_materializations_for_node("accession_count")[
        0
    ].metadata.items():
        if k == "search_word(s)":
            assert v.text == "Bacillus subtilis strain P9_B1"
