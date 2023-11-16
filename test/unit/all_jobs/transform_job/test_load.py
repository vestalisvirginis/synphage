import types

from dagster import build_op_context

from synphage.jobs import load, PipeConfig


def test_load():
    context = build_op_context()
    result = load(context, PipeConfig(source='a'))
    assert isinstance(result, types.GeneratorType)