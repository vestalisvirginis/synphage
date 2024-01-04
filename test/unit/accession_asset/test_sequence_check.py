from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.ncbi_connect.sequence_quality_assessment import sequence_check


def test_sequence_check(mock_env_sequence_check):
    context = build_asset_context()
    input_asset = [
        "test/fixtures/assets_testing_folder/sequence_quality/download/TT_000001.gb"
    ]
    result = sequence_check(context, input_asset)
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert len(result[0]) == 1
    assert isinstance(result[1], list)
    assert len(result[1]) == 1


def test_sequence_check_with_history(mock_env_sequence_check_with_history):
    context = build_asset_context()
    input_asset = [
        "test/fixtures/assets_testing_folder/sequence_quality_with_history/download/TT_000001.gb"
    ]
    result = sequence_check(context, input_asset)
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert len(result[0]) == 0
    assert isinstance(result[1], list)
    assert len(result[1]) == 1


def test_sequence_check_remove_dot(mock_env_sequence_check):
    context = build_asset_context()
    input_asset = [
        "test/fixtures/assets_testing_folder/sequence_quality/download/TT_000001.1.gb"
    ]
    result = sequence_check(context, input_asset)
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], list)
    assert len(result[0]) == 1
    assert result[0] == ["TT_000001_1.gb"]
    assert isinstance(result[1], list)
    assert len(result[1]) == 1
    assert result[1] == [
        "test/fixtures/assets_testing_folder/sequence_quality/download/TT_000001.1.gb"
    ]


def test_sequence_check_asset(mock_env_sequence_check):
    @asset(name="fetch_genome")
    def mock_upstream():
        return [
            "test/fixtures/assets_testing_folder/sequence_quality/download/TT_000001.gb"
        ]

    assets = [sequence_check, mock_upstream]
    result = materialize_to_memory(assets)
    assert result.success
    new_seq = result.output_for_node("sequence_check", "new_transferred_files")
    hist_seq = result.output_for_node("sequence_check", "history_transferred_files")
    assert new_seq == ["TT_000001.gb"]
    assert hist_seq == [
        "test/fixtures/assets_testing_folder/sequence_quality/download/TT_000001.gb"
    ]
