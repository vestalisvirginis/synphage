import polars as pl

from synphage.assets.structure_module.transform_interproscan import _filter_overlapping_domains


def _make_group(**kwargs) -> pl.DataFrame:
    return pl.DataFrame(kwargs)


def test_non_overlapping_domains_both_kept():
    """Domains on opposite ends of a phage protein are independently functional: both kept."""
    group = _make_group(
        start=[10, 150],
        stop=[110, 250],
        d1_length=[100, 100],
        score=[1e-20, 1e-10],
        domain=["Tail_fiber_N", "Phage_T7_tail"],
    )
    result = _filter_overlapping_domains(group)
    assert result.height == 2


def test_exactly_50pct_overlap_is_kept():
    """Threshold is strict >0.5: 50 % overlap must NOT be dropped.

    overlap = max(10,60)=60 → min(110,160)=110 → 50 nt; ratio=50/100=0.5 → kept.
    """
    group = _make_group(
        start=[10, 60],
        stop=[110, 160],
        d1_length=[100, 100],
        score=[1e-20, 1e-10],
        domain=["Domain_A", "Domain_B"],
    )
    result = _filter_overlapping_domains(group)
    assert result.height == 2


def test_just_over_50pct_overlap_is_dropped():
    """One position past the boundary: ratio=0.51 → domain dropped.

    overlap = max(10,59)=59 → min(110,159)=110 → 51 nt; min_len=100; ratio=0.51 > 0.5.
    """
    group = _make_group(
        start=[10, 59],
        stop=[110, 159],
        d1_length=[100, 100],
        score=[1e-20, 1e-10],
        domain=["Domain_A", "Domain_B"],
    )
    result = _filter_overlapping_domains(group)
    assert result.height == 1
    assert result["domain"][0] == "Domain_A"


def test_score_ordering_determines_winner():
    """Worst e-value arrives first in input; ascending sort elevates best e-value, which wins.

    D1 score=1e-5 (weak), D2 score=1e-30 (strong); after sort D2 is processed first and kept.
    D1 overlaps D2 by 51 % and is dropped.
    """
    group = _make_group(
        start=[10, 59],
        stop=[110, 159],
        d1_length=[100, 100],
        score=[1e-5, 1e-30],
        domain=["Pfam_Weak", "Pfam_Strong"],
    )
    result = _filter_overlapping_domains(group)
    assert result.height == 1
    assert result["domain"][0] == "Pfam_Strong"


def test_cascade_three_domains_middle_dropped():
    """D2 overlaps D1 (dropped); D3 has no overlap with D1 so both D1 and D3 survive."""
    group = _make_group(
        start=[10, 59, 200],
        stop=[110, 159, 300],
        d1_length=[100, 100, 100],
        score=[1e-25, 1e-15, 1e-8],
        domain=["Domain_A", "Domain_B", "Domain_C"],
    )
    result = _filter_overlapping_domains(group)
    assert result.height == 2
    assert set(result["domain"].to_list()) == {"Domain_A", "Domain_C"}


def test_single_domain_always_kept():
    group = _make_group(
        start=[10], stop=[100], d1_length=[90], score=[1e-20], domain=["Solo_domain"]
    )
    result = _filter_overlapping_domains(group)
    assert result.height == 1


def test_empty_group_returns_empty_dataframe():
    group = pl.DataFrame(
        schema={
            "start": pl.Int64,
            "stop": pl.Int64,
            "d1_length": pl.Int64,
            "score": pl.Float64,
            "domain": pl.Utf8,
        }
    )
    result = _filter_overlapping_domains(group)
    assert result.is_empty()
