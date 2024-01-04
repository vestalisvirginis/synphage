from synphage.assets.viewer.static_graph import CheckOrientation


def test_checkorientation():
    assert callable(CheckOrientation)
    assert hasattr(CheckOrientation, "SEQUENCE")
    assert hasattr(CheckOrientation, "REVERSE")
