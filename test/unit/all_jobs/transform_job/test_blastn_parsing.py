import os

from dagster import build_op_context

from synphage.jobs import parse_blastn, PipeConfig


SOURCE='test/fixtures/assets_testing_folder/transform/gene_identity/blastn'
TARGET='test/fixtures/assets_testing_folder/transform/fs/blastn_parsing'
TABLES='test/fixtures/assets_testing_folder/transform/tables'


def test_parse_blastn():
    context = build_op_context()
    result = parse_blastn(context, PipeConfig(source=SOURCE, target=TARGET, table_dir=TABLES, file='blastn_summary.parquet'), file='TT_000001_vs_TT_000001')
    assert isinstance(result, str)
    assert result == "OK"
    assert len(os.listdir(TARGET)) == 1


def test_parsed_blastn_content(spark):
    context = build_op_context()
    result = parse_blastn(context, PipeConfig(source=SOURCE, target=TARGET, table_dir=TABLES, file='blastn_summary.parquet'), file='TT_000001_vs_TT_000001')
    df = spark.read.parquet(TARGET+'/TT_000001_vs_TT_000001.parquet')
    assert set(
        [
            "query_genome_name",
            "query_genome_id",
            "query_gene",
            "query_locus_tag",
            "query_start_end",
            "query_gene_strand",
            "source_genome_name",
            "source_genome_id",
            "source_gene",
            "source_locus_tag",
            "source_start_end",
            "source_gene_strand",
            "percentage_of_identity",
        ]
    ).issubset(df.columns)


# Test when no match between sequences
# Parse file with errors?