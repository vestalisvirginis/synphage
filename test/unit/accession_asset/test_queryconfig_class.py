import pytest

from synphage.assets.ncbi_connect.accession import QueryConfig


def test_queryconfig_class():
    assert callable(QueryConfig)
    configuration = QueryConfig()
    assert hasattr(configuration, "search_key")
    assert hasattr(configuration, "database")
    assert hasattr(configuration, "use_history")
    assert hasattr(configuration, "idtype")
    assert hasattr(configuration, "rettype")


@pytest.mark.parametrize(
    "config, result",
    [
        [
            QueryConfig(),
            {
                "search_key": "Myoalterovirus",
                "database": "nuccore",
                "use_history": "y",
                "idtype": "acc",
                "rettype": "gb",
            },
        ],
        [
            QueryConfig(
                search_key="AAA",
                database="BBB",
                use_history="No",
                idtype="id",
                rettype="fasta",
            ),
            {
                "search_key": "AAA",
                "database": "BBB",
                "use_history": "No",
                "idtype": "id",
                "rettype": "fasta",
            },
        ],
    ],
    ids=["default", "personalised"],
)
def test_query_param(config, result):
    configuration = config
    assert configuration.dict() == result
