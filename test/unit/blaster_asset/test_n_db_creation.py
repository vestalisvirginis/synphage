import os

from pathlib import Path
from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.blaster.n_blaster import create_blast_n_db, FastaNRecord
from synphage.resources.local_resource import InputOutputConfig


TEST_DATA_FASTA_FILES = "test/fixtures/synthetic_data/fasta_files"
TEST_DATA_BLAST_DB = (
    "test/fixtures/assets_testing_folder/blasting/gene_identity/blastn_database"
)


def test_n_database_output_files(mock_env_phagy_dir_blasting):
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        }
    )
    asset_input = FastaNRecord(
        new=[str(Path(TEST_DATA_FASTA_FILES) / "TT_000001.fna")],
        history=[str(Path(TEST_DATA_FASTA_FILES) / "TT_000001.fna")],
    )
    result = create_blast_n_db(context, asset_input)
    assert isinstance(result, list)
    assert len(os.listdir(TEST_DATA_BLAST_DB)) == 7
    assert set(os.listdir(TEST_DATA_BLAST_DB)) == set(
        [
            "TT_000001.ntf",
            "TT_000001.nto",
            "TT_000001.not",
            "TT_000001.ndb",
            "TT_000001.nhr",
            "TT_000001.nin",
            "TT_000001.nsq",
        ]
    )


def test_create_blastn_db(mock_env_phagy_dir_blasting):
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        }
    )
    asset_input = FastaNRecord(
        new=[f"{Path(TEST_DATA_FASTA_FILES)} / TT_00000{i+1}.fna" for i in range(6)],
        history=[
            f"{Path(TEST_DATA_FASTA_FILES)} / TT_00000{i+1}.fna" for i in range(6)
        ],
    )
    result = create_blast_n_db(context, asset_input)
    assert isinstance(result, list)
    assert len(result) == 6
    assert set(result) == set([f"{TEST_DATA_BLAST_DB}TT_00000{i+1}" for i in range(6)])
    assert len(os.listdir(TEST_DATA_BLAST_DB)) % 7 == 0


def test_create_blastn_db_asset(mock_env_phagy_dir_blasting):
    @asset(name="new_fasta_files")
    def mock_upstream():
        return [f"{TEST_DATA_FASTA_FILES}TT_00000{i+1}.fna" for i in range(6)]

    assets = [create_blast_n_db, mock_upstream]
    result = materialize_to_memory(
        assets,
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        },
    )
    assert result.success
    all_blast_dbs = result.output_for_node("create_blast_db")
    assert len(all_blast_dbs) == 6
    assert len(os.listdir(TEST_DATA_BLAST_DB)) % 7 == 0


# additional tests on assets ___________
# output metadata for each assets
# asset config
# empty upstream asset

# additional tests on file ______________
# empty file
# error in file
# wrong format (not fasta')?
