import os

from pathlib import PosixPath, Path

from dagster import RunConfig, ExecuteInProcessResult

from synphage.jobs import transform, PipeConfig


def test_transform(mock_env_phagy_dir_transform):
    config = RunConfig(
        ops={
            "blastn": PipeConfig(
                source=str(Path(os.getenv("DATA_DIR")) / "gene_identity" / "blastn"),
                target=str(Path(os.getenv("DATA_DIR")) / "fs" / "blastn_parsing"),
                table_dir=str(Path(os.getenv("DATA_DIR")) / "tables"),
                file="blastn_summary.parquet",
            ),
            "locus": PipeConfig(
                source=str(Path(os.getenv("DATA_DIR")) / "genbank"),
                target=str(Path(os.getenv("DATA_DIR")) / "fs" / "locus_parsing"),
                table_dir=str(Path(os.getenv("DATA_DIR")) / "tables"),
                file="locus_and_gene.parquet",
            ),
        }
    )
    result = transform.execute_in_process(run_config=config)
    assert isinstance(result, ExecuteInProcessResult)
    assert result.success
    assert isinstance(result.output_for_node("blastn"), PipeConfig)
    assert isinstance(result.output_for_node("locus"), PipeConfig)
    assert isinstance(result.output_for_node("load_blastn"), dict)
    assert isinstance(result.output_for_node("load_locus"), dict)
    assert set(result.output_for_node("parse_blastn").values()) == set(["OK"])
    assert set(result.output_for_node("parse_locus").values()) == set(["OK"])
    assert isinstance(result.output_for_node("append_blastn"), PosixPath)
    assert isinstance(result.output_for_node("append_locus"), PosixPath)
    assert result.output_for_node("gene_presence") == "OK"
