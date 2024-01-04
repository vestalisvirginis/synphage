from dagster import materialize_to_memory, build_asset_context
from pathlib import PosixPath

from synphage.assets.status.gb_file_status import list_genbank_files


TEST_DATA_GB_DIR = "test/fixtures/assets_testing_folder/blasting/genbank/"


def test_list_genbank_files(mock_env_phagy_dir_blasting):
    context = build_asset_context()
    result = list_genbank_files(context)
    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert len(result[0]) == 6
    assert set(result[0]) == set(
        [PosixPath(f"{TEST_DATA_GB_DIR}TT_00000{i+1}.gb") for i in range(6)]
    )
    assert isinstance(result[1], list)
    assert len(result[1]) == 6
    assert set(result[1]) == set([f"TT_00000{i+1}" for i in range(6)])


def test_list_genbank_files_with_history(mock_env_phagy_dir_blasting_with_history):
    context = build_asset_context()
    result = list_genbank_files(context)
    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert len(result[0]) == 0
    assert set(result[0]) == set([])
    assert isinstance(result[1], list)
    assert len(result[1]) == 6
    assert set(result[1]) == set([f"TT_00000{i+1}" for i in range(6)])


def test_status_assets(mock_env_phagy_dir_blasting):
    assets = [list_genbank_files]
    result = materialize_to_memory(assets)
    assert result.success
    standardised_files = result.output_for_node(
        "list_genbank_files", "standardised_ext_file"
    )
    genbank_files = result.output_for_node("list_genbank_files", "list_genbank_files")
    assert len(standardised_files) == 6
    assert len(genbank_files) == 6


# test on output metadat?

# stop running when directory is empty?


# Test addition of files to the archive list
