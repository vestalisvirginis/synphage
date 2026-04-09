import pytest
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, MagicMock
from io import StringIO

from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.ncbi_connect.accession import fetch_genome, QueryConfig
from synphage.resources.ncbi_resource import NCBIConnection
from synphage.resources.local_resource import InputOutputConfig

ACCESSION_IDS = {
    "Count": "2",
    "RetMax": "2",
    "RetStart": "0",
    "QueryKey": "1",
    "WebEnv": "MCID_65957a742f85c1163859e8eb",
    "IdList": ["NZ_CP045811.1", "CP045811.1"],
    "TranslationSet": [
        {
            "From": "Bacillus subtilis",
            "To": '"Bacillus subtilis"[Organism] OR Bacillus subtilis[All Fields]',
        }
    ],
    "TranslationStack": [
        {
            "Term": '"Bacillus subtilis"[Organism]',
            "Field": "Organism",
            "Count": "76615",
            "Explode": "Y",
        },
        {
            "Term": "Bacillus subtilis[All Fields]",
            "Field": "All Fields",
            "Count": "285695",
            "Explode": "N",
        },
        "OR",
        "GROUP",
        {
            "Term": "strain[All Fields]",
            "Field": "All Fields",
            "Count": "146180873",
            "Explode": "N",
        },
        "AND",
        {
            "Term": "P9_B1[All Fields]",
            "Field": "All Fields",
            "Count": "184",
            "Explode": "N",
        },
        "AND",
        "GROUP",
    ],
    "QueryTranslation": '("Bacillus subtilis"[Organism] OR Bacillus subtilis[All Fields]) AND strain[All Fields] AND P9_B1[All Fields]',
}


@pytest.fixture
def temp_download_dir(monkeypatch):
    """
    Create a temporary directory with one pre-downloaded file and set environment variables.
    Added to simulate scenario where some files already exist.
    """
    temp_dir = tempfile.mkdtemp()
    download_subdir = Path(temp_dir) / "download"
    download_subdir.mkdir(exist_ok=True)

    # Create a pre-existing file (already downloaded)
    existing_file = download_subdir / "NZ_CP045811.1.gb"
    existing_file.write_text(">NZ_CP045811.1\ntest content")

    # Set environment variables for InputOutputConfig validators
    monkeypatch.setenv("INPUT_DIR", temp_dir)
    monkeypatch.setenv("OUTPUT_DIR", temp_dir)

    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def mock_ncbi_connection():
    """
    Mock NCBI connection for efetch operation.
    Added to avoid real API calls and enable controlled testing.
    """
    mock_conn = Mock()

    # Mock the efetch method to return a file-like object with genbank content
    def mock_efetch(**kwargs):
        mock_handle = StringIO(">CP045811.1\ntest genbank content")
        return mock_handle

    mock_conn.efetch.side_effect = mock_efetch

    return mock_conn


@pytest.fixture
def mock_ncbi_resource(mock_ncbi_connection):
    """
    Create a mock NCBIConnection resource.
    """
    mock_ncbi = Mock(spec=NCBIConnection)
    mock_ncbi.conn = mock_ncbi_connection
    return mock_ncbi


def test_fetch_genome(temp_download_dir, mock_ncbi_resource):
    """
    Test fetch_genome asset with mocked NCBI connection.
    Fixed to use mocked connection and temporary directory.
    """
    # set path for downstream validation
    _path = Path(temp_download_dir) / "download"
    # run validation
    context = build_asset_context(
        resources={
            "ncbi_connection": mock_ncbi_resource,
            "local_resource": InputOutputConfig(
                input_dir=temp_download_dir, output_dir=temp_download_dir
            ),
        }
    )
    ids_asset_input = ACCESSION_IDS
    downloaded_asset_input = ["NZ_CP045811.1"]
    config_input = QueryConfig(
        search_key='("Bacillus subtilis"[Organism] OR Bacillus subtilis[All Fields]) AND strain[All Fields] AND P9_B1[All Fields]'
    )
    result = fetch_genome(
        context, ids_asset_input, downloaded_asset_input, config_input
    )
    assert isinstance(result, list)
    physical_files = list(
        map(lambda x: str(_path / f"{x}.gb"), ACCESSION_IDS["IdList"])
    )
    assert len(result) == len(physical_files)
    for file in result:
        assert (
            file in physical_files
        ), f"File {file} is not present in the download folder."


def test_fetch_genome_all_downloaded(temp_download_dir, mock_ncbi_resource):
    """
    Test fetch_genome when all files have already been downloaded.
    Added to ensure edge case where no new downloads are required is handled.
    """
    # set path for downstream validation
    _path = Path(temp_download_dir) / "download"

    # Create both files that should be downloaded
    (Path(temp_download_dir) / "download" / "CP045811.1.gb").write_text(
        ">CP045811.1\ntest content"
    )

    context = build_asset_context(
        resources={
            "ncbi_connection": mock_ncbi_resource,
            "local_resource": InputOutputConfig(
                input_dir=temp_download_dir, output_dir=temp_download_dir
            ),
        }
    )
    ids_asset_input = ACCESSION_IDS
    # Both files already downloaded
    downloaded_asset_input = ["NZ_CP045811.1", "CP045811.1"]
    config_input = QueryConfig(
        search_key='("Bacillus subtilis"[Organism] OR Bacillus subtilis[All Fields]) AND strain[All Fields] AND P9_B1[All Fields]'
    )
    result = fetch_genome(
        context, ids_asset_input, downloaded_asset_input, config_input
    )
    assert isinstance(result, list)
    # Should return list of all accession IDs as genbank files
    physical_files = list(
        map(lambda x: str(_path / f"{x}.gb"), ACCESSION_IDS["IdList"])
    )
    assert len(result) == len(physical_files)
    # Verify no new downloads were attempted (ncbi_connection.efetch not called)
    assert mock_ncbi_resource.conn.efetch.call_count == 0
    """
    Test fetch_genome asset materialization with mocked NCBI connection.
    Fixed to use mocked connection and temporary directory.
    """
    # set path for downstream validation
    _path = str(Path(temp_download_dir) / "download")

    # run validation
    @asset(name="accession_ids")
    def mock_upstream_ids():
        return ACCESSION_IDS

    @asset(name="downloaded_genomes")
    def mock_upstream_download():
        return ["NZ_CP045811.1"]

    @asset(name="setup_query_config")
    def mock_config_upstream():
        return QueryConfig(
            search_key='("Bacillus subtilis"[Organism] OR Bacillus subtilis[All Fields]) AND strain[All Fields] AND P9_B1[All Fields]'
        )

    assets = [
        fetch_genome,
        mock_upstream_ids,
        mock_upstream_download,
        mock_config_upstream,
    ]
    result = materialize_to_memory(
        assets,
        resources={
            "ncbi_connection": mock_ncbi_resource,
            "local_resource": InputOutputConfig(
                input_dir=temp_download_dir, output_dir=temp_download_dir
            ),
        },
    )
    assert result.success
    genomes = result.output_for_node("fetch_genome")
    physical_files = list(map(lambda x: f"{_path}/{x}.gb", ACCESSION_IDS["IdList"]))
    assert len(genomes) == len(physical_files)
    for file in genomes:
        assert (
            file in physical_files
        ), f"File {file} is not present in the download folder."
