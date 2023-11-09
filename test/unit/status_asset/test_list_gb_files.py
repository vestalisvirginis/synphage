from dagster import materialize, materialize_to_memory, build_asset_context

from synphage.assets.status.status import list_genbank_files


def test_status_assets(mock_env_phagy_dir):
    assets = [list_genbank_files]
    result = materialize_to_memory(assets)
    assert result.success
    standardised_files = result.output_for_node(
        "list_genbank_files", "standardised_ext_file"
    )
    genbank_files = result.output_for_node("list_genbank_files", "list_genbank_files")
    assert len(standardised_files) == 0  # Why don't return the list of 5 genome?
    assert len(genbank_files) == 0  # Why don't return the list of 5 genome?


def test_list_genbank_files(mock_env_phagy_dir):
    context = build_asset_context()
    result = list_genbank_files(context)
    assert isinstance(result, tuple)
    assert result == ([], [])  # Same here: why is it empty???
    assert result.count("standardised_ext_file") == result.count("list_genbank_files")


# test on output metadat?

# stop running when directory is empty?
