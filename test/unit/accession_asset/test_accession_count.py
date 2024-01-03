import os

from pathlib import PosixPath
from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.ncbi_connect.accession import accession_count
from synphage.resources.ncbi_resource import NCBIConnection


def test_accession_count(mock_env_ncbi_count):
    context = build_asset_context(resources={"ncbi_connection": NCBIConnection(email=os.getenv("EMAIL"), api_key=os.getenv("API_KEY"))})
    result = accession_count(context)
    assert isinstance(result, int)
    assert result == 2


def test_accession_count_asset(mock_env_ncbi_count):
    assets = [accession_count]
    result = materialize_to_memory(assets, resources={"ncbi_connection": NCBIConnection(email=os.getenv("EMAIL"), api_key=os.getenv("API_KEY"))})
    assert result.success
    acc_count = result.output_for_node("accession_count")
    assert acc_count == 2
