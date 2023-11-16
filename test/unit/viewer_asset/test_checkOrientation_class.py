from synphage.assets.viewer.viewer import CheckOrientation

def test_checkorientation():
    assert callable(CheckOrientation)
    assert hasattr(CheckOrientation, "SEQUENCE")
    assert hasattr(CheckOrientation, "REVERSE")