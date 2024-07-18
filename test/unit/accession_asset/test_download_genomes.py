import os
from pathlib import Path

from dagster import build_asset_context, materialize_to_memory

from synphage.assets.ncbi_connect.accession import downloaded_genomes
from synphage.resources.local_resource import InputOutputConfig


def test_downloaded_genomes_pos(mock_env_ncbi_download_pos):
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        }
    )
    result = downloaded_genomes(context)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result == ["TT_000001"]


def test_downloded_genomes_neg(mock_env_ncbi_download_neg):
    _path = str(Path(os.getenv("DATA_DIR")) / "download")
    os.makedirs(_path, exist_ok=True)
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        }
    )
    result = downloaded_genomes(context)
    assert isinstance(result, list)
    assert len(result) == 0
    assert result == []


def test_download_genomes_asset(mock_env_ncbi_download_pos):
    assets = [downloaded_genomes]
    result = materialize_to_memory(
        assets,
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        },
    )
    assert result.success
    downloaded_files = result.output_for_node("downloaded_genomes")
    assert len(downloaded_files) == 1
