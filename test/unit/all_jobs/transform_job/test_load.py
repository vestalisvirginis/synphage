import pytest
import types

from dagster import build_op_context

# from synphage.jobs import load, PipeConfig


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_load():
    context = build_op_context()
    result = load(context, PipeConfig(source="a"))
    assert isinstance(result, types.GeneratorType)


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_load_files():
    context = build_op_context()
    result = load(
        context,
        PipeConfig(source="test/fixtures/assets_testing_folder/transform/genbank"),
    )
    assert isinstance(result, types.GeneratorType)
    assert len([r for r in result]) == 6
