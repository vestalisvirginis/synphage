from dagster import materialize_to_memory, build_asset_context

from synphage.assets.user_data.users_file_transfer import empty_dir


def test_empty_dir():
    context = build_asset_context()
    result = empty_dir(context)
    assert isinstance(result, str)
    assert result == "This directory is empty"


def test_empty_dir_asset():
    assets = [empty_dir]
    result = materialize_to_memory(assets)
    assert result.success
    empty_msg = result.output_for_node("empty_dir")
    assert isinstance(empty_msg, str)
