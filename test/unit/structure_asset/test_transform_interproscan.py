from pathlib import Path

import pandas as pd
import pytest
from dagster import build_asset_context

from synphage.assets.structure_module.transform_interproscan import transform_interproscan
from synphage.resources.local_resource import InputOutputConfig


FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures" / "structure_module"

_GENBANK_DF = pd.DataFrame({
    "key": ["prot_001", "prot_002", "prot_003"],
    "protein_id": ["YP_A", "YP_B", "YP_C"],
})


@pytest.fixture()
def interproscan_env(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("INPUT_DIR", str(tmp_path))

    tables_dir = tmp_path / "tables"
    tables_dir.mkdir()
    _GENBANK_DF.to_parquet(tables_dir / "processed_genbank_df.parquet", index=False)

    chunks_dir = tmp_path / "interpro" / "chunks"
    chunks_dir.mkdir(parents=True)

    # Two chunk files covering all three proteins in _GENBANK_DF
    chunk_1 = chunks_dir / "genome_a_chunk_1_JOBID001.tsv"
    chunk_1.write_text(
        "prot_001\tmd5\t300\tPfam\tPF00948\tTail_spike_N\t10\t110\t1.5e-30\tT\t2024-01-01\tIPR014824\tTail spike protein\t-\t-\n"
        "prot_002\tmd5\t200\tPfam\tPF03864\tMajor_capsid_E\t5\t185\t3.0e-25\tT\t2024-01-01\tIPR005468\tPhage major capsid protein\t-\t-\n",
        encoding="utf-8",
    )

    chunk_2 = chunks_dir / "genome_b_chunk_1_JOBID002.tsv"
    chunk_2.write_text(
        "prot_003\tmd5\t150\tPfam\tPF04233\tDUF429\t20\t130\t8.0e-15\tT\t2024-01-01\tIPR007325\tDomain of unknown function DUF429\t-\t-\n",
        encoding="utf-8",
    )

    return {
        "tmp_path": tmp_path,
        "tables_dir": tables_dir,
        "interpro_dir": tmp_path / "interpro",
        "chunks_dir": chunks_dir,
        "chunk_1": chunk_1,
        "chunk_2": chunk_2,
        "resource": InputOutputConfig(input_dir=str(tmp_path), output_dir=str(tmp_path)),
    }


def _context(env: dict):
    return build_asset_context(resources={"local_resource": env["resource"]})


def test_chunks_appended_to_cumulative_tsv(interproscan_env):
    f = interproscan_env
    transform_interproscan(_context(f), [str(f["chunk_1"])])
    cumulative = f["interpro_dir"] / "interproscan_results.tsv"
    assert cumulative.exists()
    content = cumulative.read_text(encoding="utf-8")
    assert "prot_001" in content
    assert "prot_002" in content


def test_merge_with_genbank_left_join(interproscan_env):
    """All genbank rows are preserved; proteins in both sources get domain annotations."""
    f = interproscan_env
    transform_interproscan(_context(f), [str(f["chunk_1"]), str(f["chunk_2"])])
    df = pd.read_parquet(f["tables_dir"] / "genbank_df_interpro.parquet")
    assert len(df) == 3
    assert df.loc[df["key"] == "prot_001", "domain"].values[0] == "Tail spike protein"
    assert df.loc[df["key"] == "prot_002", "domain"].values[0] == "Phage major capsid protein"
    assert df.loc[df["key"] == "prot_003", "domain"].values[0] == "Domain of unknown function DUF429"


def test_empty_new_list_rebuilds_from_chunks_on_disk(interproscan_env):
    """The cumulative TSV is rebuilt from every chunk file on disk, regardless of
    the upstream list. Even with an empty 'new chunks' list, all chunks present in
    the chunks directory are parsed (idempotent / self-healing behaviour)."""
    f = interproscan_env

    transform_interproscan(_context(f), [])
    df = pd.read_parquet(f["tables_dir"] / "genbank_df_interpro.parquet")
    assert len(df) == 3
    # All three proteins are annotated because their chunk files exist on disk.
    assert df.loc[df["key"] == "prot_001", "domain"].values[0] == "Tail spike protein"
    assert df.loc[df["key"] == "prot_002", "domain"].values[0] == "Phage major capsid protein"
    assert df.loc[df["key"] == "prot_003", "domain"].values[0] == "Domain of unknown function DUF429"


def test_rebuild_is_idempotent(interproscan_env):
    """Re-running with the same chunk list does not duplicate rows in the
    cumulative TSV (the previous append-based logic grew it on every run)."""
    f = interproscan_env
    cumulative = f["interpro_dir"] / "interproscan_results.tsv"

    transform_interproscan(_context(f), [str(f["chunk_1"]), str(f["chunk_2"])])
    first = cumulative.read_text(encoding="utf-8")

    transform_interproscan(_context(f), [str(f["chunk_1"]), str(f["chunk_2"])])
    second = cumulative.read_text(encoding="utf-8")

    assert first == second
    assert second.count("prot_001") == 1


def test_no_chunks_yields_empty_annotations(interproscan_env):
    """With no chunk files on disk, the asset produces an all-NaN annotation table
    without raising (graceful empty handling rather than FileNotFoundError)."""
    f = interproscan_env
    for chunk in f["chunks_dir"].glob("*.tsv"):
        chunk.unlink()

    transform_interproscan(_context(f), [])
    df = pd.read_parquet(f["tables_dir"] / "genbank_df_interpro.parquet")
    assert len(df) == 3
    assert df["domain"].isna().all()
