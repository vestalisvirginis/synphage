import os
import pytest


from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from synphage.assets.blaster import blaster as BLT


@pytest.mark.skip
def test_gene_uniqueness(spark, tmp_path):
    d = tmp_path / "gene_uniqueness"
    d.mkdir()
    p = f"{d}/perc_unique"
    record_name = ["TT_000001", "TT_000002"]
    path_to_dataset = (
        "tests/temp/test_gene_presence_table0/gene_uniqueness/gene_uniqueness"
    )
    rs = BLT.gene_uniqueness(spark, record_name, path_to_dataset, p)
    df = spark.read.parquet(p)
    assert len(os.listdir(d)) == 1
    assert set(
        ["name", "gene", "locus_tag", "total_seq", "count", "perc_presence"]
    ) == set(df.columns)
