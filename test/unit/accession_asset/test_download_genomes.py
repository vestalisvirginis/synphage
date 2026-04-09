import os
import pytest
from pathlib import Path
import tempfile
import shutil

from dagster import build_asset_context, materialize_to_memory

from synphage.assets.ncbi_connect.accession import downloaded_genomes
from synphage.resources.local_resource import InputOutputConfig


@pytest.fixture
def temp_download_dir(monkeypatch):
    """
    Create a temporary directory with test files and set environment variables.
    Added to enable proper testing of file discovery in download directory.
    """
    temp_dir = tempfile.mkdtemp()
    download_subdir = Path(temp_dir) / "download"
    download_subdir.mkdir(exist_ok=True)

    # Create a test file
    test_file = download_subdir / "TT_000001.gb"
    test_file.write_text("test content")

    # Set environment variables for InputOutputConfig validators
    monkeypatch.setenv("INPUT_DIR", temp_dir)
    monkeypatch.setenv("OUTPUT_DIR", temp_dir)

    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def temp_empty_download_dir(monkeypatch):
    """
    Create a temporary empty directory for testing empty download scenario.
    Added to test edge case where no files have been downloaded yet.
    """
    temp_dir = tempfile.mkdtemp()
    download_subdir = Path(temp_dir) / "download"
    download_subdir.mkdir(exist_ok=True)

    # Set environment variables for InputOutputConfig validators
    monkeypatch.setenv("INPUT_DIR", temp_dir)
    monkeypatch.setenv("OUTPUT_DIR", temp_dir)

    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_downloaded_genomes_pos(temp_download_dir):
    """
    Test downloaded_genomes asset with files present in download directory.
    Fixed to use temporary directory instead of requiring fixture setup.
    """
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=temp_download_dir, output_dir=temp_download_dir
            )
        }
    )
    result = downloaded_genomes(context)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result == ["TT_000001"]


def test_downloaded_genomes_neg(temp_empty_download_dir):
    """
    Test downloaded_genomes asset with empty download directory.
    Fixed to use temporary directory instead of requiring fixture setup.
    """
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


def test_download_genomes_asset(temp_download_dir):
    """
    Test downloaded_genomes asset materialization.
    Fixed to use temporary directory instead of requiring fixture setup.
    """
    assets = [downloaded_genomes]
    result = materialize_to_memory(
        assets,
        resources={
            "local_resource": InputOutputConfig(
                input_dir=temp_download_dir, output_dir=temp_download_dir
            )
        },
    )
    assert result.success
    downloaded_files = result.output_for_node("downloaded_genomes")
    assert len(downloaded_files) == 1
