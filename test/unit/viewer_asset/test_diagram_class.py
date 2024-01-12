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
    assert hasattr(configuration, "output_folder")
    assert hasattr(configuration, "blastn_dir")
    assert hasattr(configuration, "uniq_dir")


@pytest.mark.parametrize(
    "config, result",
    [
        [
            Diagram(),
            {
                "title": "synteny_plot",
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
                "output_folder": "synteny",
                "blastn_dir": "tables/blastn_summary.parquet",
                "uniq_dir": "tables/uniqueness.parquet",
            },
        ],
        [
            Diagram(
                title="test_title",
                colours=["B", "R", "V", "N", "Y", "O", "W"],
                gradient="R",
                output_format="png",
                graph_shape="fmt",
                graph_pagesize="size",
                graph_fragments=3,
                graph_start=30000,
                graph_end=130000,
                output_folder="a",
                blastn_dir="b.parquet",
                uniq_dir="c.parquet",
            ),
            {
                "title": "test_title",
                "colours": ["B", "R", "V", "N", "Y", "O", "W"],
                "gradient": "R",
                "output_format": "png",
                "graph_shape": "fmt",
                "graph_pagesize": "size",
                "graph_fragments": 3,
                "graph_start": 30000,
                "graph_end": 130000,
                "output_folder": "a",
                "blastn_dir": "b.parquet",
                "uniq_dir": "c.parquet",
            },
        ],
    ],
    ids=["default", "personalised"],
)
def test_diagram_param(config, result):
    configuration = config
    assert configuration.dict() == result
