import pytest
import os
import polars as pl

from dagster import build_op_context

# from synphage.jobs import parse_locus, PipeConfig


SOURCE = "test/fixtures/assets_testing_folder/transform/genbank"
TARGET = "test/fixtures/assets_testing_folder/transform/fs/locus_parsing"
TABLES = "test/fixtures/assets_testing_folder/transform/tables"

SOURCE_NEG = "test/fixtures/negative/synthetic_data"


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_parse_locus_result():
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


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_parse_locus_content():
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
    df = pl.read_parquet(TARGET + "/TT_000001.parquet")
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(pl.col("gene").str.len_chars().eq(0)).cast(pl.Int8).sum().item() == 0
    )  # No missing gene name
    assert (
        df.select(pl.col("locus_tag").str.len_chars().eq(0)).cast(pl.Int8).sum().item()
        == 0
    )  # No missing locus tag
    assert df.select(pl.count()).item() == 5  # 5 entries


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_parse_locus_content_cds():
    context = build_op_context()
    result = parse_locus(
        context,
        PipeConfig(
            source=SOURCE,
            target=TARGET,
            table_dir=TABLES,
            file="locus_and_gene.parquet",
        ),
        file="TT_000006.gb",
    )
    df = pl.read_parquet(TARGET + "/TT_000006.parquet")
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(pl.col("gene").str.len_chars().eq(0)).cast(pl.Int8).sum().item() == 0
    )  # No missing gene name
    assert (
        df.select(pl.col("locus_tag").str.len_chars().eq(0)).cast(pl.Int8).sum().item()
        == 0
    )  # No missing locus tag
    assert df.select(pl.count()).item() == 4  # 4 entries


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_parse_locus_content_missing_gene_name_value():
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
    df = pl.read_parquet(TARGET + "/TT_000001_missing_gene_name_value.parquet")
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(pl.col("gene").str.len_chars().eq(0)).cast(pl.Int8).sum().item() == 1
    )  # One missing gene name
    assert (
        df.select(pl.col("locus_tag").str.len_chars().eq(0)).cast(pl.Int8).sum().item()
        == 0
    )  # No missing locus tag
    assert df.select(pl.count()).item() == 5  # 5 entries


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_parse_locus_content_missing_gene_name_key():
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
    df = pl.read_parquet(TARGET + "/TT_000001_missing_gene_name_key.parquet")
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(pl.col("gene").str.len_chars().eq(0)).cast(pl.Int8).sum().item() == 1
    )  # One missing gene name
    assert (
        df.select(pl.col("locus_tag").str.len_chars().eq(0)).cast(pl.Int8).sum().item()
        == 0
    )  # No missing locus tag
    assert df.select(pl.count()).item() == 5  # 5 entries


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_parse_locus_content_missing_locus_tag_value():
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
    df = pl.read_parquet(TARGET + "/TT_000001_missing_locus_tag_value.parquet")
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(pl.col("gene").str.len_chars().eq(0)).cast(pl.Int8).sum().item() == 0
    )  # One missing gene name
    assert (
        df.select(pl.col("locus_tag").str.len_chars().eq(0)).cast(pl.Int8).sum().item()
        == 1
    )  # No missing locus tag
    assert df.select(pl.count()).item() == 5  # 5 entries


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_parse_locus_content_missing_locus_tag_key():
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
    df = pl.read_parquet(TARGET + "/TT_000001_missing_locus_tag_key.parquet")
    assert set(["name", "gene", "locus_tag"]) == set(df.columns)
    assert (
        df.select(pl.col("gene").str.len_chars().eq(0)).cast(pl.Int8).sum().item() == 0
    )  # One missing gene name
    assert (
        df.select(pl.col("locus_tag").str.len_chars().eq(0)).cast(pl.Int8).sum().item()
        == 1
    )  # No missing locus tag
    assert df.select(pl.count()).item() == 5  # 5 entries
