import pytest
import polars as pl

from synphage.assets.viewer.static_graph import gene_uniqueness


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_gene_uniqueness():
    _path = "test/fixtures/viewer/gene_uniq_neg/part-00000-86ebc250-cc19-4372-9ed1-a818f67af698-c000.snappy.parquet"
    result = gene_uniqueness(_path, ["A", "B"])
    assert isinstance(result, pl.DataFrame)
    assert set(result.columns).issuperset(
        ["name", "gene", "locus_tag", "total_seq", "count", "perc_presence"]
    )


# # test perc_presence value?
