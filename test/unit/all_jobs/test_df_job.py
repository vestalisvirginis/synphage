import pytest

from pydantic import ValidationError

from synphage.jobs import setup


def test_se():
    pass


#     # test_config = PipeConfig(
#     #     source='a',
#     # )
#     rs = setup(PipeConfig(source="a"))
#     # assert rs.dict() == {'source': 'a', 'target': None, 'table_dir': None, 'file': 'out.parquet'}

#     # def test_op_with_config():
#     # assert op_requires_config(MyOpConfig(my_int=5)) == 10
