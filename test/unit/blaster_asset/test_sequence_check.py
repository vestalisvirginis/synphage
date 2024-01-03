import os
import pytest

from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.blaster.blaster import sequence_check


def test_sequence_check(mock_env_sequence_check):
    context = build_asset_context()
    input_asset = ['test/fixtures/assets_testing_folder/sequence_quality/download/TT_000001.gb']
    result = sequence_check(context, input_asset)
    assert isinstance(result, list)
    assert len(result) == 1


def test_sequence_check_asset(mock_env_sequence_check):
    @asset(name="fetch_genome")
    def mock_upstream():
        return ['test/fixtures/assets_testing_folder/sequence_quality/download/TT_000001.gb']
    
    assets = [sequence_check, mock_upstream]
    result = materialize_to_memory(assets)
    assert result.success
    seq = result.output_for_node("sequence_check")
    assert seq == ['TT_000001']