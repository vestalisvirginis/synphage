from dagster import materialize, materialize_to_memory, build_asset_context
from pathlib import PosixPath

from synphage.assets.status.status import list_genbank_files


def test_status_assets(mock_env_phagy_dir):
    assets = [list_genbank_files]
    result = materialize_to_memory(assets)
    assert result.success
    standardised_files = result.output_for_node("list_genbank_files", "standardised_ext_file")
    genbank_files = result.output_for_node("list_genbank_files", "list_genbank_files")
    assert len(standardised_files) == 5
    assert len(genbank_files) == 5


def test_list_genbank_files(mock_env_phagy_dir):
    context = build_asset_context()
    result = list_genbank_files(context)
    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert len(result[0]) == 5
    assert set(result[0]) == set([PosixPath('test/fixtures/synthetic_data/genbank/TT_000001.gb'), PosixPath('test/fixtures/synthetic_data/genbank/TT_000002.gb'), PosixPath('test/fixtures/synthetic_data/genbank/TT_000003.gb'), PosixPath('test/fixtures/synthetic_data/genbank/TT_000004.gb'), PosixPath('test/fixtures/synthetic_data/genbank/TT_000005.gb')])
    assert isinstance(result[1], list)
    assert len(result[1]) == 5
    assert set(result[1]) == set(['TT_000001', 'TT_000002', 'TT_000003', 'TT_000004', 'TT_000005'])


# test on output metadat?

# stop running when directory is empty?
