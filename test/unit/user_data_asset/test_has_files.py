import pytest

from types import GeneratorType as generator

from dagster import (
    materialize_to_memory,
    build_asset_context,
    DagsterInvariantViolationError,
)

from synphage.assets.user_data.users_file_transfer import has_files
from synphage.resources.local_resource import InputOutputConfig


@pytest.mark.parametrize(
    "input_path",
    [
        ("test/fixtures/user_data_transfer/user_data"),
        ("test/fixtures/user_data_transfer"),
    ],
    ids=["files_in_dir", "empty_dir"],
)
def test_has_files(input_path):
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=input_path, output_dir="test/fixtures/user_data_transfer/data"
            )
        }
    )
    result = has_files(context)
    assert (result, generator)


@pytest.mark.parametrize(
    "input_path, node, skipped",
    [
        ("test/fixtures/user_data_transfer/user_data", "files", "empty"),
        ("test/fixtures/user_data_transfer", "empty", "files"),
    ],
    ids=["files_in_dir", "empty_dir"],
)
def test_has_files_asset(input_path, node, skipped):
    assets = [has_files]
    result = materialize_to_memory(
        assets,
        resources={
            "local_resource": InputOutputConfig(
                input_dir=input_path, output_dir="test/fixtures/user_data_transfer/data"
            )
        },
    )
    assert result.success
    successful_node = result.output_for_node("has_files", node)
    assert successful_node is None
    with pytest.raises(
        DagsterInvariantViolationError,
        match=f"No outputs found for output '{skipped}' from node 'has_files'.",
    ):
        result.output_for_node("has_files", skipped)
