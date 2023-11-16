import pytest

from Bio import SeqIO
from Bio.SeqFeature import SeqFeature

from synphage.assets.viewer.viewer import _get_feature


def test_get_feature_positive():
    path = "test/fixtures/viewer/sequences/TT_000001.gb"
    feature_id = "GEN_10"
    features = SeqIO.read(path, "gb").features
    result = _get_feature(features, feature_id)
    assert isinstance(result, SeqFeature)


def test_get_feature_negative():
    path = "test/fixtures/viewer/sequences/TT_000001.gb"
    feature_id = "NEG_10"
    features = SeqIO.read(path, "gb").features
    with pytest.raises(KeyError, match="NEG_10"):
        _get_feature(features, feature_id)


@pytest.mark.parametrize(
    "path, feature_id",
    [
        ["test/fixtures/viewer/sequences/TT_000001.gb", "GEN_10"],
        ["test/fixtures/viewer/sequences/TT_000001.gb", "geneA"],
        ["test/fixtures/viewer/sequences/TT_000001.gb", "GEN10"],
        ["test/fixtures/viewer/sequences/TT_000006.gb", "NP_000061"],
    ],
    ids=[
        "locus_tag",
        "gene",
        "old_locus_tag",
        "protein_id",
    ],
)
def test_get_feature_parameters(path, feature_id):
    path = path
    feature_id = feature_id
    features = SeqIO.read(path, "gb").features
    result = _get_feature(features, feature_id)
    assert isinstance(result, SeqFeature)
