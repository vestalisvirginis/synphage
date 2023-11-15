from pyspark.sql import DataFrame

from synphage.assets.viewer.viewer import gene_uniqueness


def test_gene_uniqueness(spark):
    path = 'test/fixtures/viewer/gene_uniq_neg'
    result = gene_uniqueness(spark, ['A', 'B'], path)
    assert isinstance(result, DataFrame)
    assert set(result.columns).issuperset(["name", "gene", "locus_tag", 'total_seq', 'count', 'perc_presence'])

# test perc_presence value?
