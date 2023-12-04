import pytest


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
