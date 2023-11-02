import os
from pathlib import Path

from synphage.assets.status.status import _standardise_file_extention


def test_standardise_file_extension_positive():
    # initialise extension to .gbk
    _path_file = Path("test/fixtures/genbank/my_file_pos.gb")
    _path_file.rename(_path_file.with_suffix(".gbk"))
    assert os.path.exists("test/fixtures/genbank/my_file_pos.gbk")
    # test change from .gbk to .gb
    _target_file = "test/fixtures/genbank/my_file_pos.gbk"
    rs = _standardise_file_extention(_target_file)
    assert rs.name == "my_file_pos.gb"
    assert os.path.exists("test/fixtures/genbank/my_file_pos.gb")


def test_standardise_file_extension_negative():
    _target_file = "test/fixtures/genbank/my_file_neg.gb"
    rs = _standardise_file_extention(_target_file)
    assert rs.name == "my_file_neg.gb"
    assert os.path.exists("test/fixtures/genbank/my_file_neg.gb")
