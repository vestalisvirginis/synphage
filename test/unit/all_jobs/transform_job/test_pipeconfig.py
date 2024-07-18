# import pytest

# from pydantic import ValidationError

# #from synphage.jobs import PipeConfig


# @pytest.mark.skip(reason="need to rewrite test to accomodate changes")
# def test_pipeconfig_pos():
#     assert callable(PipeConfig)
#     configuration = PipeConfig(
#         source="a",
#         target="b",
#         table_dir="c",
#         file="d.parquet",
#     )
#     assert hasattr(configuration, "source")
#     assert hasattr(configuration, "target")
#     assert hasattr(configuration, "table_dir")
#     assert hasattr(configuration, "file")


# @pytest.mark.skip(reason="need to rewrite test to accomodate changes")
# def test_pipeconfig_neg():
#     with pytest.raises(ValidationError, match="1 validation error for PipeConfig"):
#         PipeConfig()


# @pytest.mark.parametrize(
#     "config, result",
#     [
#         [
#             PipeConfig(source="a"),
#             {"source": "a", "target": None, "table_dir": None, "file": "out.parquet"},
#         ],
#         [
#             PipeConfig(source="a", target="b"),
#             {"source": "a", "target": "b", "table_dir": None, "file": "out.parquet"},
#         ],
#         [
#             PipeConfig(source="a", target="b", table_dir="c"),
#             {"source": "a", "target": "b", "table_dir": "c", "file": "out.parquet"},
#         ],
#         [
#             PipeConfig(source="a", target="b", table_dir="c", file="d.parquet"),
#             {"source": "a", "target": "b", "table_dir": "c", "file": "d.parquet"},
#         ],
#     ],
#     ids=["source_value", "target_value", "table_dir", "file"],
# )
# def test_pipeconfig_param(config, result):
#     configuration = config
#     assert configuration.dict() == result
