import pytest

from synphage.assets.viewer.static_graph import Diagram


def test_diagram_class():
    assert callable(Diagram)
    configuration = Diagram()
    assert hasattr(configuration, "title")

    assert hasattr(configuration, "colours")
    assert hasattr(configuration, "gradient")
    assert hasattr(configuration, "output_format")
    assert hasattr(configuration, "graph_shape")
    assert hasattr(configuration, "graph_pagesize")
    assert hasattr(configuration, "graph_fragments")
    assert hasattr(configuration, "graph_start")
    assert hasattr(configuration, "graph_end")


@pytest.mark.parametrize(
    "config, result",
    [
        [
            Diagram(),
            {
                "title": "synteny_plot",
                "graph_type": "blastn",
                "colours": [
                    "#fde725",
                    "#90d743",
                    "#35b779",
                    "#21918c",
                    "#31688e",
                    "#443983",
                    "#440154",
                ],
                "gradient": "#B22222",
                "output_format": "SVG",
                "graph_shape": "linear",
                "graph_pagesize": "A4",
                "graph_fragments": 1,
                "graph_start": 0,
                "graph_end": None,
            },
        ],
        [
            Diagram(
                title="test_title",
                graph_type="blastp",
                colours=["B", "R", "V", "N", "Y", "O", "W"],
                gradient="R",
                output_format="png",
                graph_shape="fmt",
                graph_pagesize="size",
                graph_fragments=3,
                graph_start=30000,
                graph_end=130000,
            ),
            {
                "title": "test_title",
                "graph_type": "blastp",
                "colours": ["B", "R", "V", "N", "Y", "O", "W"],
                "gradient": "R",
                "output_format": "png",
                "graph_shape": "fmt",
                "graph_pagesize": "size",
                "graph_fragments": 3,
                "graph_start": 30000,
                "graph_end": 130000,
            },
        ],
    ],
    ids=["default", "personalised"],
)
def test_diagram_param(config, result):
    configuration = config
    assert configuration.dict() == result
