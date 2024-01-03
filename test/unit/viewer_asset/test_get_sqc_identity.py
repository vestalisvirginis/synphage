from synphage.assets.viewer.viewer import _get_sqc_identity_from_csv


def test_get_sqc_identity_from_csv():
    path = "test/fixtures/viewer/sequences_files/sequences.csv"
    result = _get_sqc_identity_from_csv(path)
    assert isinstance(result, dict)
    assert {'TT_000001.gb': 0, 'TT_000002.gb': 1}

# # Test for correctness of the string
# # Test for checking int value is 0 or 1
