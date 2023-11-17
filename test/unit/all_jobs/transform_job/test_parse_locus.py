import pytest
import os

import pyspark.sql.functions as F

from dagster import build_op_context

from synphage.jobs import parse_locus, PipeConfig


SOURCE = "test/fixtures/assets_testing_folder/transform/genbank"
TARGET = "test/fixtures/assets_testing_folder/transform/fs/locus_parsing"
TABLES = "test/fixtures/assets_testing_folder/transform/tables"

SOURCE_NEG = "test/fixtures/negative/synthetic_data"


def test_parse_locus():
    context = build_op_context()
    result = parse_locus(
        context,
        PipeConfig(
            source=SOURCE,
            target=TARGET,
            table_dir=TABLES,
            file="locus_and_gene.parquet",
        ),
        file="TT_000001.gb",
    )
    assert isinstance(result, str)
    assert result == "OK"
    assert len(os.listdir(TARGET)) == 1


def test_parse_locus_content(spark):
    context = build_op_context()
    result = parse_locus(
        context,
        PipeConfig(
            source=SOURCE,
            target=TARGET,
            table_dir=TABLES,
            file="locus_and_gene.parquet",
        ),
        file="TT_000001.gb",
    )
    df = spark.read.parquet(TARGET + "/TT_000001.parquet")
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(F.sum((F.col("gene") == "").cast("integer"))).collect()[0][0] == 0
    )  # No missing gene name
    assert (
        df.select(F.sum((F.col("locus_tag") == "").cast("integer"))).collect()[0][0]
        == 0
    )  # No missing locus tag
    assert df.count() == 5  # 5 entries


def test_parse_locus_content_missing_gene_name_value(spark):
    context = build_op_context()
    result = parse_locus(
        context,
        PipeConfig(
            source=SOURCE_NEG,
            target=TARGET,
            table_dir=TABLES,
            file="locus_and_gene.parquet",
        ),
        file="TT_000001_missing_gene_name_value.gb",
    )
    df = spark.read.parquet(TARGET + "/TT_000001_missing_gene_name_value.parquet")
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(F.sum((F.col("gene") == "").cast("integer"))).collect()[0][0] == 1
    )  # One missing gene name
    assert (
        df.select(F.sum((F.col("locus_tag") == "").cast("integer"))).collect()[0][0]
        == 0
    )  # No missing locus tag
    assert df.count() == 5  # 5 entries


def test_parse_locus_content_missing_gene_name_key(spark):
    context = build_op_context()
    result = parse_locus(
        context,
        PipeConfig(
            source=SOURCE_NEG,
            target=TARGET,
            table_dir=TABLES,
            file="locus_and_gene.parquet",
        ),
        file="TT_000001_missing_gene_name_key.gb",
    )
    df = spark.read.parquet(TARGET + "/TT_000001_missing_gene_name_key.parquet")
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(F.sum((F.col("gene") == "").cast("integer"))).collect()[0][0] == 1
    )  # One missing gene name
    assert (
        df.select(F.sum((F.col("locus_tag") == "").cast("integer"))).collect()[0][0]
        == 0
    )  # No missing locus tag
    assert df.count() == 5  # 5 entries


def test_parse_locus_content_missing_locus_tag_value(spark):
    context = build_op_context()
    result = parse_locus(
        context,
        PipeConfig(
            source=SOURCE_NEG,
            target=TARGET,
            table_dir=TABLES,
            file="locus_and_gene.parquet",
        ),
        file="TT_000001_missing_locus_tag_value.gb",
    )
    df = spark.read.parquet(TARGET + "/TT_000001_missing_locus_tag_value.parquet")
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(F.sum((F.col("gene") == "").cast("integer"))).collect()[0][0] == 0
    )  # One missing gene name
    assert (
        df.select(F.sum((F.col("locus_tag") == "").cast("integer"))).collect()[0][0]
        == 1
    )  # No missing locus tag
    assert df.count() == 5  # 5 entries


def test_parse_locus_content_missing_locus_tag_key(spark):
    context = build_op_context()
    result = parse_locus(
        context,
        PipeConfig(
            source=SOURCE_NEG,
            target=TARGET,
            table_dir=TABLES,
            file="locus_and_gene.parquet",
        ),
        file="TT_000001_missing_locus_tag_key.gb",
    )
    df = spark.read.parquet(TARGET + "/TT_000001_missing_locus_tag_key.parquet")
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(F.sum((F.col("gene") == "").cast("integer"))).collect()[0][0] == 0
    )  # One missing gene name
    assert (
        df.select(F.sum((F.col("locus_tag") == "").cast("integer"))).collect()[0][0]
        == 1
    )  # No missing locus tag
    assert df.count() == 5  # 5 entries
