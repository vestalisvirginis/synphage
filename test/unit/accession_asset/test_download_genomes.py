import os
from pathlib import Path

from dagster import build_asset_context, materialize_to_memory, asset

from synphage.assets.ncbi_connect.accession import downloaded_genomes, QueryConfig


def test_downloaded_genomes_pos(mock_env_ncbi_download_pos):
    context = build_asset_context()
    input_config = QueryConfig()
    result = downloaded_genomes(context, input_config)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result == ["TT_000001"]


def test_downloded_genomes_neg(mock_env_ncbi_download_neg):
    _path = str(Path(os.getenv("DATA_DIR")) / "download")
    os.makedirs(_path, exist_ok=True)
    context = build_asset_context()
    input_config = QueryConfig()
    result = downloaded_genomes(context, input_config)
    assert isinstance(result, list)
    assert len(result) == 0
    assert result == []


def test_download_genomes_asset(mock_env_ncbi_download_pos):
    @asset(name="setup_query_config")
    def mock_config_upstream():
        return QueryConfig()

    assets = [downloaded_genomes, mock_config_upstream]
    result = materialize_to_memory(assets)
    assert result.success
    downloaded_files = result.output_for_node("downloaded_genomes")
    assert len(downloaded_files) == 1
