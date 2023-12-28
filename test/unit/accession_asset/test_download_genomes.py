from dagster import build_asset_context, materialize_to_memory

from synphage.assets.ncbi_connect.accession import downloaded_genomes


def test_downloaded_genomes_pos(mock_env_ncbi_download_pos):
    context = build_asset_context()
    result = downloaded_genomes(context)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result == ["TT_000001"]


def test_downloded_genomes_neg(mock_env_ncbi_download_neg):
    context = build_asset_context()
    result = downloaded_genomes(context)
    assert isinstance(result, list)
    assert len(result) == 0
    assert result == []


def test_download_genomes_asset(mock_env_ncbi_download_pos):
    assets = [downloaded_genomes]
    result = materialize_to_memory(assets)
    assert result.success
    downloaded_files = result.output_for_node("downloaded_genomes")
    assert len(downloaded_files) == 1
