from pathlib import Path

import pandas as pd
import pytest
from dagster import AssetCheckResult, Output, build_asset_context

from synphage.assets.structure_module.transform_foldseek import transform_foldseek
from synphage.resources.local_resource import InputOutputConfig


FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures" / "structure_module"

_GENBANK_DF = pd.DataFrame({
    "key": ["abc123", "def456", "ghi789", "jkl012", "mno345"],
    "protein_id": ["YP_001", "YP_002", "YP_003", "YP_004", "YP_005"],
    "genome": ["GenomeA", "GenomeA", "GenomeB", "GenomeB", "GenomeC"],
})


@pytest.fixture()
def foldseek_env(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("INPUT_DIR", str(tmp_path))

    tables_dir = tmp_path / "tables"
    tables_dir.mkdir()
    _GENBANK_DF.to_parquet(tables_dir / "processed_genbank_df.parquet", index=False)

    cluster_tsv = FIXTURE_DIR / "foldseek" / "results_cluster.tsv"

    return {
        "tmp_path": tmp_path,
        "tables_dir": tables_dir,
        "cluster_tsv": cluster_tsv,
        "resource": InputOutputConfig(input_dir=str(tmp_path), output_dir=str(tmp_path)),
    }


def _run(env: dict) -> tuple[pd.DataFrame, list[AssetCheckResult]]:
    context = build_asset_context(resources={"local_resource": env["resource"]})
    outputs = list(transform_foldseek(context, str(env["cluster_tsv"])))
    df = pd.read_parquet(env["tables_dir"] / "genbank_df_clustered.parquet")
    checks = [o for o in outputs if isinstance(o, AssetCheckResult)]
    return df, checks


def test_cluster_sizes_correct(foldseek_env):
    df, _ = _run(foldseek_env)
    assert df.loc[df["key"] == "abc123", "cluster_size"].values[0] == 2
    assert df.loc[df["key"] == "def456", "cluster_size"].values[0] == 2
    assert df.loc[df["key"] == "ghi789", "cluster_size"].values[0] == 2
    assert df.loc[df["key"] == "jkl012", "cluster_size"].values[0] == 2
    assert df.loc[df["key"] == "mno345", "cluster_size"].values[0] == 1


def test_representative_protein_id_lookup(foldseek_env):
    """Non-representative cluster members must resolve to the rep's protein_id."""
    df, _ = _run(foldseek_env)
    assert df.loc[df["key"] == "def456", "representative"].values[0] == "YP_001"
    assert df.loc[df["key"] == "jkl012", "representative"].values[0] == "YP_003"
    assert df.loc[df["key"] == "mno345", "representative"].values[0] == "YP_005"


def test_all_genbank_rows_preserved(foldseek_env):
    df, _ = _run(foldseek_env)
    assert len(df) == 5


def test_row_count_check_passes_when_tsv_matches(foldseek_env):
    _, checks = _run(foldseek_env)
    check = next(c for c in checks if c.check_name == "cluster_tsv_row_count_matches")
    assert check.passed


def test_row_count_check_fails_when_tsv_shorter(foldseek_env):
    """A cluster TSV with fewer rows than the protein count fails the check.
    The genbank fixture has 5 proteins; this TSV covers only 3 keys."""
    f = foldseek_env
    short_tsv = f["tmp_path"] / "short_cluster.tsv"
    short_tsv.write_text(
        "abc123\tabc123\nabc123\tdef456\nghi789\tghi789\n", encoding="utf-8"
    )

    context = build_asset_context(resources={"local_resource": f["resource"]})
    outputs = list(transform_foldseek(context, str(short_tsv)))
    checks = [o for o in outputs if isinstance(o, AssetCheckResult)]
    check = next(c for c in checks if c.check_name == "cluster_tsv_row_count_matches")
    assert not check.passed


def test_row_count_check_uses_protein_count_not_genbank_rows(tmp_path, monkeypatch):
    """Gene-only rows (null translation_fn) are not sent to Foldseek, so the check
    baseline is the protein count, not the raw genbank row count. Here 4 genbank
    rows contain only 3 proteins, and the cluster TSV has 3 rows -> check passes."""
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("INPUT_DIR", str(tmp_path))

    tables_dir = tmp_path / "tables"
    tables_dir.mkdir()
    pd.DataFrame({
        "key": ["k1", "k2", "k3", "k4"],
        "protein_id": ["YP_1", "YP_2", "YP_3", None],
        "translation_fn": ["MAA", "MBB", "MCC", None],
    }).to_parquet(tables_dir / "processed_genbank_df.parquet", index=False)

    cluster_tsv = tmp_path / "results_cluster.tsv"
    cluster_tsv.write_text("k1\tk1\nk1\tk2\nk3\tk3\n", encoding="utf-8")

    resource = InputOutputConfig(input_dir=str(tmp_path), output_dir=str(tmp_path))
    context = build_asset_context(resources={"local_resource": resource})
    outputs = list(transform_foldseek(context, str(cluster_tsv)))
    checks = [o for o in outputs if isinstance(o, AssetCheckResult)]
    check = next(c for c in checks if c.check_name == "cluster_tsv_row_count_matches")
    assert check.passed


def test_no_missing_values_check_passes(foldseek_env):
    _, checks = _run(foldseek_env)
    check = next(c for c in checks if c.check_name == "cluster_tsv_no_missing_values")
    assert check.passed


def test_string_keys_not_coerced_to_int(tmp_path, monkeypatch):
    """Keys like '001' must survive the TSV merge as strings, not be cast to integers."""
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("INPUT_DIR", str(tmp_path))

    tables_dir = tmp_path / "tables"
    tables_dir.mkdir()
    pd.DataFrame({
        "key": ["001", "002", "003"],
        "protein_id": ["YP_A", "YP_B", "YP_C"],
    }).to_parquet(tables_dir / "processed_genbank_df.parquet", index=False)

    cluster_tsv = tmp_path / "results_cluster.tsv"
    cluster_tsv.write_text("001\t001\n001\t002\n003\t003\n", encoding="utf-8")

    resource = InputOutputConfig(input_dir=str(tmp_path), output_dir=str(tmp_path))
    context = build_asset_context(resources={"local_resource": resource})
    list(transform_foldseek(context, str(cluster_tsv)))

    df = pd.read_parquet(tables_dir / "genbank_df_clustered.parquet")
    assert df.loc[df["key"] == "001", "cluster_size"].values[0] == 2
    assert df.loc[df["key"] == "002", "cluster_size"].values[0] == 2
    assert df.loc[df["key"] == "003", "cluster_size"].values[0] == 1
