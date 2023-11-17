from pathlib import Path, PosixPath

from synphage.jobs import append, PipeConfig


TABLES = "test/fixtures/assets_testing_folder/transform_2/tables"
TARGET_BN = "test/fixtures/assets_testing_folder/transform_2/fs/blastn_parsing"
TARGET_LT = "test/fixtures/assets_testing_folder/transform_2/fs/locus_parsing"
FILE_BN = "blastn_summary.parquet"
FILE_LT = "locus_and_gene.parquet"


def test_append_blastn():
    result = append(PipeConfig(source='a', target=TARGET_BN, table_dir=TABLES, file=FILE_BN))
    assert isinstance(result, Path)
    assert result == PosixPath("test/fixtures/assets_testing_folder/transform_2/tables/blastn_summary.parquet")


def test_append_locus_tag():
    result = append(PipeConfig(source='a', target=TARGET_LT, table_dir=TABLES, file=FILE_LT))
    assert isinstance(result, Path)
    assert result == PosixPath("test/fixtures/assets_testing_folder/transform_2/tables/locus_and_gene.parquet")
