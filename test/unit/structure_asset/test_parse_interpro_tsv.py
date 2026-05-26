from pathlib import Path

from synphage.assets.structure_module.transform_interproscan import _parse_interpro_tsv


FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures" / "structure_module"


def _interpro_row(
    key: str = "prot_001",
    domain: str = "Tail_spike_protein",
    start: str = "10",
    stop: str = "110",
    score: str = "1.5e-20",
) -> str:
    return "\t".join([
        key,
        "d41d8cd98f00b204e980",
        "300",
        "Pfam",
        "PF00948",
        "Tail_spike_N",
        start,
        stop,
        score,
        "T",
        "2024-01-01",
        "IPR014824",
        domain,
        "-",
        "-",
    ])


def test_basic_parsing_output_columns(tmp_path):
    tsv = tmp_path / "result.tsv"
    tsv.write_text(_interpro_row() + "\n", encoding="utf-8")
    result = _parse_interpro_tsv(tsv)
    expected = {"key", "domain", "d1_length", "n_domains"}
    assert expected.issubset(set(result.columns))
    assert result.height == 1


def test_domain_dash_rows_filtered(tmp_path):
    """Rows with domain=='-' have no InterPro family assignment and must be excluded."""
    tsv = tmp_path / "result.tsv"
    tsv.write_text(
        _interpro_row(key="prot_001", domain="Real Domain", start="10", stop="110") + "\n"
        + _interpro_row(key="prot_001", domain="-", start="200", stop="250") + "\n",
        encoding="utf-8",
    )
    result = _parse_interpro_tsv(tsv)
    assert result.height == 1
    assert result["domain"][0] == "Real Domain"


def test_d1_length_equals_stop_minus_start(tmp_path):
    tsv = tmp_path / "result.tsv"
    tsv.write_text(_interpro_row(start="10", stop="110") + "\n", encoding="utf-8")
    result = _parse_interpro_tsv(tsv)
    assert result["d1_length"][0] == 100


def test_all_dash_returns_empty_structured_dataframe(tmp_path):
    """All hits lack an InterPro assignment: function must return an empty but schema-correct DataFrame."""
    tsv = tmp_path / "result.tsv"
    tsv.write_text(
        _interpro_row(key="prot_001", domain="-") + "\n"
        + _interpro_row(key="prot_002", domain="-") + "\n",
        encoding="utf-8",
    )
    result = _parse_interpro_tsv(tsv)
    assert result.is_empty()
    required = {"key", "domain", "d1_length", "n_domains"}
    assert required.issubset(set(result.columns))


def test_single_domain_protein_has_null_domain_2_and_3(tmp_path):
    tsv = tmp_path / "result.tsv"
    tsv.write_text(_interpro_row(key="prot_001", domain="Domain_A") + "\n", encoding="utf-8")
    result = _parse_interpro_tsv(tsv)
    assert result["n_domains"][0] == 1
    assert result["domain_2"][0] is None
    assert result["domain_3"][0] is None


def test_three_non_overlapping_domains_all_reported(tmp_path):
    """Phage protein with three non-overlapping Pfam hits: all three reported in rank order."""
    tsv = tmp_path / "result.tsv"
    rows = "\n".join([
        _interpro_row(key="prot_001", domain="Domain_A", start="10", stop="110", score="1e-30"),
        _interpro_row(key="prot_001", domain="Domain_B", start="120", stop="220", score="1e-20"),
        _interpro_row(key="prot_001", domain="Domain_C", start="230", stop="330", score="1e-10"),
    ]) + "\n"
    tsv.write_text(rows, encoding="utf-8")
    result = _parse_interpro_tsv(tsv)
    assert result.height == 1
    assert result["domain"][0] == "Domain_A"
    assert result["domain_2"][0] == "Domain_B"
    assert result["domain_3"][0] == "Domain_C"
    assert result["n_domains"][0] == 3


def test_overlapping_domains_only_best_survives():
    """Load real fixture with two overlapping GPCR hits: only the best e-value survives.

    D1 (score=1e-20) and D2 (score=5e-10) overlap by 51/100 = 0.51 > 0.5.
    D2 is dropped; n_domains must be 1.
    """
    tsv_path = FIXTURE_DIR / "interpro" / "chunk_overlapping_domains.tsv"
    result = _parse_interpro_tsv(tsv_path)
    assert result.height == 1
    assert result["domain"][0] == "G-protein coupled receptor"
    assert result["n_domains"][0] == 1
