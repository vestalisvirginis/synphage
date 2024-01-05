import pytest

from synphage.assets.viewer.static_graph import Genome


def test_genome_class():
    assert callable(Genome)
    configuration = Genome()
    assert hasattr(configuration, "sequence_file")


@pytest.mark.parametrize(
    "config, result",
    [
        [
            Genome(),
            {
                "sequence_file": "sequences.csv",
            },
        ],
        [
            Genome(
                sequence_file="test_sequence_file_name",
            ),
            {
                "sequence_file": "test_sequence_file_name",
            },
        ],
    ],
    ids=["default", "personalised"],
)
def test_genome_class_config(config, result):
    configuration = config
    assert configuration.dict() == result
