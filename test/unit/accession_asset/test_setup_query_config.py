import os

from dagster import materialize_to_memory, build_asset_context, asset, RunConfig

from synphage.assets.ncbi_connect.accession import setup_query_config, QueryConfig


def test_setup_query_config():
    result = setup_query_config(QueryConfig())
    assert isinstance(result, QueryConfig)


def test_setup_query_config_asset():
    assets = [setup_query_config]
    result = materialize_to_memory(
        assets,
        run_config=RunConfig({"setup_query_config": QueryConfig()}),
    )
    assert result.success
    result_config = result.output_for_node("setup_query_config")
    assert result_config == QueryConfig()
