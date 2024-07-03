import os

from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.ncbi_connect.downloaded_file_transfer import (
    download_to_genbank,
    DownloadRecord,
)
from synphage.resources.local_resource import InputOutputConfig


def test_download_to_genbank(mock_env_download_to_genbank):
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        }
    )
    input_asset = [
        "test/fixtures/assets_testing_folder/download_to_genbank/download/TT_000001.gb"
    ]
    result = download_to_genbank(context, input_asset)
    assert isinstance(result, DownloadRecord)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert len(result[0]) == 1
    assert isinstance(result[1], list)
    assert len(result[1]) == 1


def test_download_to_genbank_with_history(mock_env_download_to_genbank_with_history):
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        }
    )
    input_asset = [
        "test/fixtures/assets_testing_folder/download_to_genbank_with_history/download/TT_000001.gb"
    ]
    result = download_to_genbank(context, input_asset)
    assert isinstance(result, DownloadRecord)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert len(result[0]) == 0
    assert isinstance(result[1], list)
    assert len(result[1]) == 1


def test_download_to_genbank_remove_dot(mock_env_download_to_genbank):
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        }
    )
    input_asset = [
        "test/fixtures/assets_testing_folder/download_to_genbank/download/TT_000001.1.gb"
    ]
    result = download_to_genbank(context, input_asset)
    assert isinstance(result, DownloadRecord)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert len(result[0]) == 1
    assert result[0] == ["TT_000001_1.gb"]
    assert isinstance(result[1], list)
    assert len(result[1]) == 1
    assert result[1] == [
        "test/fixtures/assets_testing_folder/download_to_genbank/download/TT_000001.1.gb"
    ]


def test_download_to_genbank_asset(mock_env_download_to_genbank):
    @asset(name="fetch_genome")
    def mock_upstream():
        return [
            "test/fixtures/assets_testing_folder/download_to_genbank/download/TT_000001.gb"
        ]

    assets = [download_to_genbank, mock_upstream]
    result = materialize_to_memory(
        assets,
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        },
    )
    assert result.success
    sequences = result.output_for_node("download_to_genbank")
    assert sequences.new == ["TT_000001.gb"]
    assert sequences.history == [
        "test/fixtures/assets_testing_folder/download_to_genbank/download/TT_000001.gb"
    ]
