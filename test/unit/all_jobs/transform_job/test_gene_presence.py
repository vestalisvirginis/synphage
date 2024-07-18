import pytest
import os
import polars as pl

from pathlib import Path
from dagster import build_op_context

from synphage.jobs import gene_presence


BLASTN_TABLE = (
    "test/fixtures/assets_testing_folder/transform_3/tables/blastn_summary.parquet"
)
LOCUS_TAG_TABLE = (
    "test/fixtures/assets_testing_folder/transform_3/tables/locus_and_gene.parquet"
)


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_gene_presence_table(mock_env_phagy_dir_transform_step3):
    context = build_op_context()
    result = gene_presence(
        context,
        BLASTN_TABLE,
        LOCUS_TAG_TABLE,
    )
    assert isinstance(result, str)
    assert result == "OK"


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_gene_presence_table_content(mock_env_phagy_dir_transform_step3):
    context = build_op_context()
    result = gene_presence(
        context,
        BLASTN_TABLE,
        LOCUS_TAG_TABLE,
    )
    df = pl.read_parquet(Path(os.getenv("DATA_DIR")) / "tables" / "uniqueness.parquet")
    assert set(["name", "locus_tag", "gene"]).issubset(df.columns)
