import pytest
from pathlib import PosixPath
from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.blaster.n_blaster_old import genbank_to_fasta


TEST_DATA_GB_DIR = "test/fixtures/assets_testing_folder/blasting/genbank/"
TEST_DATA_FASTA_DIR = (
    "test/fixtures/assets_testing_folder/blasting/gene_identity/fasta/"
)


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_fasta_metadata_gene(mock_env_phagy_dir_blasting):
    context = build_asset_context()
    asset_input = [PosixPath(f"{TEST_DATA_GB_DIR}TT_000001.gb")]
    result = genbank_to_fasta(context, asset_input)
    assert isinstance(result, tuple)
    assert [row for row in open(f"{TEST_DATA_FASTA_DIR}TT_000001.fna")] == [
        row for row in open("test/fixtures/synthetic_data/fasta_files/TT_000001.fna")
    ]


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_fasta_metadata_cds_only(mock_env_phagy_dir_blasting):
    context = build_asset_context()
    asset_input = [PosixPath(f"{TEST_DATA_GB_DIR}TT_000006.gb")]
    result = genbank_to_fasta(context, asset_input)
    assert isinstance(result, tuple)
    assert [row for row in open(f"{TEST_DATA_FASTA_DIR}TT_000006.fna")] == [
        row for row in open("test/fixtures/synthetic_data/fasta_files/TT_000006.fna")
    ]


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_genbank_to_fasta(mock_env_phagy_dir_blasting):
    context = build_asset_context()
    asset_input = [PosixPath(f"{TEST_DATA_GB_DIR}TT_00000{i+1}.gb") for i in range(6)]
    result = genbank_to_fasta(context, asset_input)
    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert len(result[0]) == 6
    assert set(result[0]) == set(
        [f"{TEST_DATA_FASTA_DIR}TT_00000{i+1}.fna" for i in range(6)]
    )
    assert isinstance(result[1], list)
    assert len(result[1]) == 6
    assert set(result[1]) == set([f"TT_00000{i+1}" for i in range(6)])


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_genbank_to_fasta_with_history(mock_env_phagy_dir_blasting_with_history):
    context = build_asset_context()
    asset_input = [PosixPath(f"{TEST_DATA_GB_DIR}TT_00000{i+1}.gb") for i in range(6)]
    result = genbank_to_fasta(context, asset_input)
    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert len(result[0]) == 0
    assert set(result[0]) == set([])
    assert isinstance(result[1], list)
    assert len(result[1]) == 6
    assert set(result[1]) == set([f"TT_00000{i+1}" for i in range(6)])


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_genbank_to_fasta_assets(mock_env_phagy_dir_blasting):
    @asset(name="standardised_ext_file")
    def mock_upstream():
        return [PosixPath(f"{TEST_DATA_GB_DIR}TT_00000{i+1}.gb") for i in range(6)]

    assets = [genbank_to_fasta, mock_upstream]
    result = materialize_to_memory(assets)
    assert result.success
    new_fasta_files = result.output_for_node("genbank_to_fasta", "new_fasta_files")
    all_fasta_files = result.output_for_node("genbank_to_fasta", "history_fasta_files")
    assert len(new_fasta_files) == 6
    assert len(all_fasta_files) == 6


# code coverage missing
# pickle open fasta history

# additional tests on assets ___________
# output metadata for each assets
# asset config
# empty upstream asset

# additional tests on file ______________
# empty file
# error in file
# wrong format (not genbank')
