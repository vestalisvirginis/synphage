from Bio.Graphics import GenomeDiagram

from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.viewer.static_graph import create_graph, Diagram


def test_create_graph_default(mock_env_phagy_dir_synteny):
    context = build_asset_context()
    asset_input = {"TT_000001.gb": "SEQUENCE", "TT_000002.gb": "SEQUENCE"}
    result = create_graph(context, asset_input, Diagram())
    assert isinstance(result, GenomeDiagram.Diagram)


def test_create_graph_default_options(mock_env_phagy_dir_synteny_var):
    context = build_asset_context()
    asset_input = {"TT_000001.gb": "SEQUENCE", "TT_000002.gb": "SEQUENCE"}
    result = create_graph(context, asset_input, Diagram())
    assert isinstance(result, GenomeDiagram.Diagram)


def test_create_graph_asset(mock_env_phagy_dir_synteny):
    @asset(name="create_genome")
    def mock_upstream():
        return {"TT_000001.gb": "SEQUENCE", "TT_000002.gb": "SEQUENCE"}

    assets = [create_graph, mock_upstream]
    result = materialize_to_memory(assets)
    assert result.success
    plot = result.output_for_node("create_graph")
    assert isinstance(plot, GenomeDiagram.Diagram)


#     # check loggin
#     # check metadata

#     # when configurable:
#     # - logic colour
#     # - logic format
#     # - ...
