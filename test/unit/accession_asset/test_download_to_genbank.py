import pytest
import os
import tempfile
import shutil
import pickle
from pathlib import Path

from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.ncbi_connect.downloaded_file_transfer import (
    download_to_genbank,
    DownloadRecord,
)
from synphage.resources.local_resource import InputOutputConfig


@pytest.fixture
def temp_download_dir_with_file(monkeypatch):
    """
    Create a temporary directory with test genbank file and set environment variables.
    Added to test file transfer without history.
    """
    temp_dir = tempfile.mkdtemp()
    download_subdir = Path(temp_dir) / "download"
    download_subdir.mkdir(exist_ok=True)

    # Create a test genbank file
    test_file = download_subdir / "TT_000001.gb"
    test_file.write_text("LOCUS       TT_000001\n")

    # Set environment variables for InputOutputConfig validators
    monkeypatch.setenv("INPUT_DIR", temp_dir)
    monkeypatch.setenv("OUTPUT_DIR", temp_dir)

    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def temp_download_dir_with_history(monkeypatch):
    """
    Create a temporary directory with test file and history, and set environment variables.
    Added to test file transfer with pre-existing history.
    """
    temp_dir = tempfile.mkdtemp()
    download_subdir = Path(temp_dir) / "download"
    download_subdir.mkdir(exist_ok=True)
    fs_subdir = Path(temp_dir) / "fs"
    fs_subdir.mkdir(exist_ok=True)

    # Create a test genbank file
    test_file = download_subdir / "TT_000001.gb"
    test_file.write_text("LOCUS       TT_000001\n")

    # Create a history file
    history_file = fs_subdir / "download_to_genbank"
    history_data = DownloadRecord(new=[], history=[str(test_file)])
    pickle.dump(history_data, open(history_file, "wb"))

    # Set environment variables for InputOutputConfig validators
    monkeypatch.setenv("INPUT_DIR", temp_dir)
    monkeypatch.setenv("OUTPUT_DIR", temp_dir)

    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_download_to_genbank(temp_download_dir_with_file):
    """
    Test download_to_genbank asset without history.
    Fixed to use temporary directory with proper file setup.
    """
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=temp_download_dir_with_file,
                output_dir=temp_download_dir_with_file,
            )
        }
    )
    input_asset = [str(Path(temp_download_dir_with_file) / "download" / "TT_000001.gb")]
    result = download_to_genbank(context, input_asset)
    assert isinstance(result, DownloadRecord)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert len(result[0]) == 1
    assert isinstance(result[1], list)
    assert len(result[1]) == 1


def test_download_to_genbank_with_history(temp_download_dir_with_history):
    """
    Test download_to_genbank asset with pre-existing history.
    Fixed to use temporary directory with history file.
    """
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=temp_download_dir_with_history,
                output_dir=temp_download_dir_with_history,
            )
        }
    )
    input_asset = [
        str(Path(temp_download_dir_with_history) / "download" / "TT_000001.gb")
    ]
    result = download_to_genbank(context, input_asset)
    assert isinstance(result, DownloadRecord)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert len(result[0]) == 0
    assert isinstance(result[1], list)
    assert len(result[1]) == 1


@pytest.mark.parametrize(
    "filename, new_name",
    [
        (
            "TT_000001.1.gb",
            "TT_000001_1.gb",
        ),
        (
            "TT 00000 1.gb",
            "TT_00000_1.gb",
        ),
    ],
    ids=["filename_with_dot", "file_name_with_space"],
)
def test_download_to_genbank_rename(
    temp_download_dir_with_file, monkeypatch, filename, new_name
):
    """
    Test that filename harmonization works correctly.
    Fixed to use temporary directory and parametrized test cases.
    """
    # Create the specific test file
    download_subdir = Path(temp_download_dir_with_file) / "download"
    test_file = download_subdir / filename
    test_file.write_text(f"LOCUS       {filename}\n")

    # Re-set environment variables to ensure they're still set
    monkeypatch.setenv("INPUT_DIR", temp_download_dir_with_file)
    monkeypatch.setenv("OUTPUT_DIR", temp_download_dir_with_file)

    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=temp_download_dir_with_file,
                output_dir=temp_download_dir_with_file,
            )
        }
    )
    input_asset = [str(test_file)]
    result = download_to_genbank(context, input_asset)
    assert isinstance(result, DownloadRecord)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert len(result[0]) == 1
    assert result[0] == [new_name]
    assert isinstance(result[1], list)
    assert len(result[1]) == 1
    assert result[1] == [str(test_file)]


def test_download_to_genbank_asset(temp_download_dir_with_file):
    """
    Test download_to_genbank asset materialization.
    Fixed to use temporary directory.
    """

    @asset(name="fetch_genome")
    def mock_upstream():
        return [str(Path(temp_download_dir_with_file) / "download" / "TT_000001.gb")]

    assets = [download_to_genbank, mock_upstream]
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
    sequences = result.output_for_node("download_to_genbank")
    assert sequences.new == ["TT_000001.gb"]
    assert sequences.history == [
        str(Path(temp_download_dir_with_file) / "download" / "TT_000001.gb")
    ]
