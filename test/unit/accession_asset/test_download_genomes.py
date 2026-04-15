from dagster import build_asset_context, materialize_to_memory

from synphage.assets.ncbi_connect.accession import downloaded_genomes
from synphage.resources.local_resource import InputOutputConfig


def test_downloaded_genomes_pos(temp_download_dir_with_file):
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=temp_download_dir_with_file,
                output_dir=temp_download_dir_with_file,
            )
        }
    )
    result = downloaded_genomes(context)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result == ["TT_000001"]


def test_downloaded_genomes_neg(temp_empty_download_dir):
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=temp_empty_download_dir, output_dir=temp_empty_download_dir
            )
        }
    )
    result = downloaded_genomes(context)
    assert isinstance(result, list)
    assert len(result) == 0
    assert result == []


def test_download_genomes_asset(temp_download_dir_with_file):
    assets = [downloaded_genomes]
    result = materialize_to_memory(
        assets,
        resources={
            "local_resource": InputOutputConfig(
                input_dir=temp_download_dir_with_file,
                output_dir=temp_download_dir_with_file,
            )
        },
    )
    assert result.success
    downloaded_files = result.output_for_node("downloaded_genomes")
    assert len(downloaded_files) == 1
