import os
import pytest


from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from synphage.assets.blaster import blaster as BLT


@pytest.mark.skip
def test_extract_locus_tag_gene_positive(spark, tmp_path):
    path = "tests/fixtures/synthetic_data/genbank/TT_000001.gb"
    d = tmp_path / "locus_and_gene"
    d.mkdir()
    p = f"{d}/locus_and_gene"
    rs = BLT.extract_locus_tag_gene(spark, path, p)
    df = spark.read.parquet(p)
    assert len(os.listdir(d)) == 1
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(F.sum((F.col("gene") == "").cast("integer"))).collect()[0][0] == 0
    )  # No missing gene name
    assert (
        df.select(F.sum((F.col("gene") == "").cast("integer"))).collect()[0][0] == 0
    )  # No missing locus tag
    assert df.count() == 5  # 5 entries


@pytest.mark.skip
def test_extract_locus_tag_gene_multiple_files(spark, tmp_path):
    path_1 = "tests/fixtures/synthetic_data/genbank/TT_000001.gb"
    path_2 = "tests/fixtures/synthetic_data/genbank/TT_000002.gb"
    d = tmp_path / "locus_and_gene"
    d.mkdir()
    p = f"{d}/locus_and_gene_2"
    rs_1 = BLT.extract_locus_tag_gene(spark, path_1, p)
    rs_2 = BLT.extract_locus_tag_gene(spark, path_2, p)
    df = spark.read.parquet(p)
    assert (
        len(os.listdir(d)) == 1
    )  # 1 file  because data are appended in the same parquet directory
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert df.count() == 12  # 5 entries from file 1 and 7 entries from file 2


@pytest.mark.skip
def test_extract_locus_tag_gene_repeated_file(spark, tmp_path):
    path = "tests/fixtures/synthetic_data/genbank/TT_000001.gb"
    d = tmp_path / "locus_and_gene"
    d.mkdir()
    p = f"{d}/locus_and_gene"
    rs_1 = BLT.extract_locus_tag_gene(spark, path, p)
    rs_2 = BLT.extract_locus_tag_gene(spark, path, p)
    df = spark.read.parquet(p)
    assert (
        len(os.listdir(d)) == 1
    )  # 1 file  because data are appended in the same parquet directory
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert df.count() == 10  # 5 entries from file 1 duplicated!
    assert df.dropDuplicates().count() == 5


@pytest.mark.skip
def test_extract_locus_tag_gene_missing_gene_name_value(spark, tmp_path):
    path = "tests/fixtures/negative/synthetic_data/TT_000001_missing_gene_name_value.gb"
    d = tmp_path / "locus_and_gene"
    d.mkdir()
    p = f"{d}/locus_and_gene"
    rs = BLT.extract_locus_tag_gene(spark, path, p)
    df = spark.read.parquet(p)
    assert len(os.listdir(d)) == 1
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(F.sum((F.col("gene") == "").cast("integer"))).collect()[0][0] == 1
    )  # One missing gene name
    assert (
        df.select(F.sum((F.col("locus_tag") == "").cast("integer"))).collect()[0][0]
        == 0
    )  # No missing locus tag
    assert df.count() == 5  # 5 entries


@pytest.mark.skip
def test_extract_locus_tag_gene_missing_locus_tag_value(spark, tmp_path):
    path = "tests/fixtures/negative/synthetic_data/TT_000001_missing_locus_tag_value.gb"
    d = tmp_path / "locus_and_gene"
    d.mkdir()
    p = f"{d}/locus_and_gene"
    rs = BLT.extract_locus_tag_gene(spark, path, p)
    df = spark.read.parquet(p)
    assert len(os.listdir(d)) == 1
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(F.sum((F.col("gene") == "").cast("integer"))).collect()[0][0] == 0
    )  # No missing gene name
    assert (
        df.select(F.sum((F.col("locus_tag") == "").cast("integer"))).collect()[0][0]
        == 1
    )  # One missing locus tag
    assert df.count() == 5  # 5 entries


@pytest.mark.skip
def test_extract_locus_tag_gene_missing_gene_name_key(spark, tmp_path):
    path = "tests/fixtures/negative/synthetic_data/TT_000001_missing_gene_name_key.gb"
    d = tmp_path / "locus_and_gene"
    d.mkdir()
    p = f"{d}/locus_and_gene"
    rs = BLT.extract_locus_tag_gene(spark, path, p)
    df = spark.read.parquet(p)
    assert len(os.listdir(d)) == 1
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(F.sum((F.col("gene") == "").cast("integer"))).collect()[0][0] == 1
    )  # One missing gene name
    assert (
        df.select(F.sum((F.col("locus_tag") == "").cast("integer"))).collect()[0][0]
        == 0
    )  # No missing locus tag
    assert df.count() == 5  # 5 entries


@pytest.mark.skip
def test_extract_locus_tag_gene_missing_locus_tag_key(spark, tmp_path):
    path = "tests/fixtures/negative/synthetic_data/TT_000001_missing_locus_tag_key.gb"
    d = tmp_path / "locus_and_gene"
    d.mkdir()
    p = f"{d}/locus_and_gene"
    rs = BLT.extract_locus_tag_gene(spark, path, p)
    df = spark.read.parquet(p)
    assert len(os.listdir(d)) == 1
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(F.sum((F.col("gene") == "").cast("integer"))).collect()[0][0] == 0
    )  # No missing gene name
    assert (
        df.select(F.sum((F.col("locus_tag") == "").cast("integer"))).collect()[0][0]
        == 1
    )  # One missing locus tag
    assert df.count() == 5  # 5 entries


@pytest.mark.skip
def test_gene_presence_table(spark, tmp_path):
    path_locus = (
        "tests/temp/test_extract_locus_tag_gene_po0/locus_and_gene/locus_and_gene"
    )
    path_blastn = "tests/temp/test_parse_blastn0/blastn_summary/blastn_summary"
    d = tmp_path / "gene_uniqueness"
    d.mkdir()
    p = f"{d}/gene_uniqueness"
    rs = BLT.gene_presence_table(spark, path_locus, path_blastn, p)
    df = spark.read.parquet(p)
    assert len(os.listdir(d)) == 1
    assert set(["name", "locus_tag", "gene"]).issubset(df.columns)


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
