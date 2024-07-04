import os

from dagster import materialize_to_memory, build_asset_context

from synphage.assets.user_data.users_file_transfer import (
    users_to_genbank,
    UsersRecord,
)
from synphage.resources.local_resource import InputOutputConfig


def test_users_to_genbank(mock_env_users_to_genbank):
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        }
    )
    result = users_to_genbank(context)
    assert isinstance(result, UsersRecord)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert len(result[0]) == 1
    assert isinstance(result[1], list)
    assert len(result[1]) == 1


def test_users_to_genbank_with_history(mock_env_users_to_genbank_with_history):
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        }
    )
    result = users_to_genbank(context)
    assert isinstance(result, UsersRecord)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert len(result[0]) == 0
    assert isinstance(result[1], list)
    assert len(result[1]) == 1


def test_users_to_genbank_remove_dot(mock_env_users_to_genbank):
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir="test/fixtures/user_data_transfer/user_data_with_dot",
                output_dir=os.getenv("OUTPUT_DIR"),
            )
        }
    )
    result = users_to_genbank(context)
    assert isinstance(result, UsersRecord)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert len(result[0]) == 1
    assert result[0] == ["TT_000002_1.gb"]
    assert isinstance(result[1], list)
    assert len(result[1]) == 1
    assert result[1] == [
        "test/fixtures/user_data_transfer/user_data_with_dot/TT_000002.1.gb"
    ]


def test_users_to_genbank_asset(mock_env_users_to_genbank):
    assets = [users_to_genbank]
    result = materialize_to_memory(
        assets,
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("DATA_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        },
    )
    assert result.success
    sequences = result.output_for_node("users_to_genbank")
    assert sequences.new == ["TT_000001.gb"]
    assert sequences.history == [
        "test/fixtures/user_data_transfer/user_data/TT_000001.gb"
    ]
