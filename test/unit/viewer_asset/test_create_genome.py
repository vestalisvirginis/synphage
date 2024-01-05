from dagster import materialize_to_memory, build_asset_context

from synphage.assets.viewer.static_graph import create_genome, Genome


def test_create_genome(mock_env_phagy_dir_synteny):
    context = build_asset_context()
    result = create_genome(context, config=Genome())
    assert isinstance(result, dict)
    assert [k for k in result.keys()] == ["TT_000001.gb", "TT_000002.gb"]
    assert [v for v in result.values()] == ["SEQUENCE", "REVERSE"]


def test_create_genome_asset(mock_env_phagy_dir_synteny):
    asset = [create_genome]
    result = materialize_to_memory(
        asset,
        run_config={
            "ops": {"create_genome": {"config": {"sequence_file": "sequences.csv"}}}
        },
    )
    assert result.success
    sequences = result.output_for_node("create_genome")
    assert len(sequences) == 2
    assert [k for k in sequences.keys()] == ["TT_000001.gb", "TT_000002.gb"]
    assert [v for v in sequences.values()] == ["SEQUENCE", "REVERSE"]


# # Check Metadata
# # Logger
# # file exist or no! + File format
