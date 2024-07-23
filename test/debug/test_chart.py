from synphage.assets import viewer as VW
from dagster import build_asset_context
from synphage.resources.local_resource import InputOutputConfig
import os


def test_uno():
    context = build_asset_context(
        resources={
            "local_resource": InputOutputConfig(
                input_dir=os.getenv("INPUT_DIR"), output_dir=os.getenv("OUTPUT_DIR")
            )
        }
    )

    genome = VW.static_graph.Genome()
    diagram = VW.static_graph.Diagram()

    input_asset_1 = "OK"
    input_asset_2 = "OK"

    make_genome = VW.static_graph.create_genome(
        context, genome, input_asset_1, input_asset_2
    )
    graph = VW.static_graph.create_graph(context, make_genome, diagram)
