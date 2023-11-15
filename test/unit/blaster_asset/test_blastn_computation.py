import pytest
import re

from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.blaster.blaster import get_blastn


TEST_DATASET_BLAST_DB = (
    "test/fixtures/synthetic_data/blast_db/"
) 


def test_get_blastn(mock_env_phagy_dir_blasting):
    context = build_asset_context()
    asset_input_fasta = [f"TT_00000{i+1}" for i in range(6)]
    asset_input_dbs = [f"{TEST_DATASET_BLAST_DB}TT_00000{i+1}" for i in range(6)]
    result = get_blastn(context, asset_input_fasta, asset_input_dbs)
    assert isinstance(result, list)
    assert len(result) == 36
    #assert set() file names


def test_get_blastn_asset(mock_env_phagy_dir_blasting):
    @asset(name="history_fasta_files")
    def mock_upstream_fasta():
        return [f"TT_00000{i+1}" for i in range(6)]
    @asset(name="create_blast_db")
    def mock_upstream_dbs():
        return [f"{TEST_DATASET_BLAST_DB}TT_00000{i+1}" for i in range(6)]

    assets = [get_blastn, mock_upstream_fasta, mock_upstream_dbs]
    result = materialize_to_memory(assets)
    assert result.success
    blastn_files = result.output_for_node("get_blastn")
    assert len(blastn_files) == 36
    assert [re.search('_vs_', file_name) for file_name in blastn_files]



@pytest.mark.skip
def test_blastn(tmp_path):
    d = tmp_path / "blastn_results"
    d.mkdir()
    p = f"{d}/TT_000001_vs_TT_000002"
    rs = BLT.get_blastn(
        "tests/fixtures/synthetic_data/fasta/TT_000002.fna",
        "tests/temp/test_blast_database_positive0/database/TT_000001",
        p,
    )
    assert len(os.listdir(d)) == 1
    assert os.listdir(d) == ["TT_000001_vs_TT_000002"]




# test if _history_path exist

# additional tests on assets ___________
# output metadata for each assets
# asset config
# empty upstream asset

# additional tests on file ______________
# empty file
# error in file
# wrong format (not genbank')