import pytest
from Bio.Graphics import GenomeDiagram

from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.viewer.static_graph import create_graph, Diagram


def test_create_graph_default(mock_env_phagy_dir_synteny):
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


@pytest.mark.parametrize(
    "title, config_param",
    [
        [{"title": "default_plot"}, {}],
        [
            {"title": "param_colour_1"},
            {
                "colours": [
                    "#440154",
                    "#443983",
                    "#31688e",
                    "#21918c",
                    "#35b779",
                    "#90d743",
                    "#fde725",
                ]
            },
        ],
        [
            {"title": "param_colour_2"},
            {
                "colours": [
                    "#440154",
                    "#443983",
                    "#31688e",
                    "#21918c",
                    "#35b779",
                    "#90d743",
                    "#fde725",
                    "#FFFFFF",
                    "#B22222",
                ]
            },
        ],
        [
            {"title": "param_colour_3"},
            {"colours": ["#440154", "#443983", "#31688e", "#21918c"]},
        ],
        [{"title": "param_gradient_1"}, {"gradient": ["#FFFFFF", "#C0C0C0"]}],
        [
            {"title": "param_gradient_2"},
            {"gradient": ["#40E0D0", "#FF6347", "#4682B4"]},
        ],
        [{"title": "param_gradient_3"}, {"gradient": ["#FA8072"]}],
        [{"title": "param_shape"}, {"graph_shape": "circular"}],
    ],
    ids=[
        "default",
        "colour_palette",
        "colour_palette_too_big",
        "colour_palette_missing_values",
        "gradient",
        "gradient_too_many_values",
        "gradient_missing_values",
        "graph_shape",
    ],
)
def test_create_graph_asset_config(mock_env_phagy_dir_synteny, title, config_param):
    @asset(name="create_genome")
    def mock_upstream():
        return {"TT_000001.gb": "SEQUENCE", "TT_000002.gb": "SEQUENCE"}

    assets = [create_graph, mock_upstream]
    result = materialize_to_memory(
        assets,
        run_config={"ops": {"create_graph": {"config": {**title, **config_param}}}},
    )
    assert result.success
    plot = result.output_for_node("create_graph")
    assert isinstance(plot, GenomeDiagram.Diagram)


#     # check loggin
#     # check metadata

#     # when configurable:
#     # - logic colour
#     # - logic format
#     # - ...
