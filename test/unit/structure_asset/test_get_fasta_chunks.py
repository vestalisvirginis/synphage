from pathlib import Path

from synphage.assets.structure_module.interpro_scan import _get_fasta_chunks


_SEQ = "MKTAYIAKQRQISFVKSHFSRQLEERLGLIEVQAPILSRVGDGTQDNLSGAEKAVQVK"


def _write_fasta(path: Path, n_seqs: int) -> None:
    lines: list[str] = []
    for i in range(n_seqs):
        lines.append(f">protein_{i:04d}")
        lines.append(_SEQ)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_few_sequences_single_chunk(tmp_path):
    fasta = tmp_path / "proteins.fasta"
    _write_fasta(fasta, 3)
    chunks, skipped = _get_fasta_chunks(fasta, max_seqs=99)
    assert skipped == 0
    assert len(chunks) == 1
    assert chunks[0].count(">") == 3


def test_exact_boundary_no_split(tmp_path):
    """99 sequences with max_seqs=99 must stay in one chunk (boundary is not a split point)."""
    fasta = tmp_path / "proteins.fasta"
    _write_fasta(fasta, 99)
    chunks, skipped = _get_fasta_chunks(fasta, max_seqs=99)
    assert skipped == 0
    assert len(chunks) == 1
    assert chunks[0].count(">") == 99


def test_one_over_boundary_creates_two_chunks(tmp_path):
    """100 sequences split as 99 + 1 — the InterProScan REST API limit in practice."""
    fasta = tmp_path / "proteins.fasta"
    _write_fasta(fasta, 100)
    chunks, skipped = _get_fasta_chunks(fasta, max_seqs=99)
    assert skipped == 0
    assert len(chunks) == 2
    assert chunks[0].count(">") == 99
    assert chunks[1].count(">") == 1


def test_two_full_chunks_198_sequences(tmp_path):
    fasta = tmp_path / "proteins.fasta"
    _write_fasta(fasta, 198)
    chunks, skipped = _get_fasta_chunks(fasta, max_seqs=99)
    assert skipped == 0
    assert len(chunks) == 2
    assert all(c.count(">") == 99 for c in chunks)


def test_sequence_content_preserved(tmp_path):
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">seq_alpha\nMAAA\n>seq_beta\nMKKK\n", encoding="utf-8")
    chunks, skipped = _get_fasta_chunks(fasta, max_seqs=99)
    assert skipped == 0
    assert ">seq_alpha\n" in chunks[0]
    assert "MAAA\n" in chunks[0]
    assert ">seq_beta\n" in chunks[0]
    assert "MKKK\n" in chunks[0]


def test_empty_record_is_skipped(tmp_path):
    """A header with no sequence line must be dropped, not emitted into a chunk."""
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(
        ">seq_good\nMAAA\n>seq_empty\n>seq_also_good\nMKKK\n", encoding="utf-8"
    )
    chunks, skipped = _get_fasta_chunks(fasta, max_seqs=99)
    assert skipped == 1
    assert len(chunks) == 1
    assert chunks[0].count(">") == 2
    assert "seq_empty" not in chunks[0]


def test_too_short_record_is_skipped(tmp_path):
    """A 1-2 residue sequence is rejected by InterProScan and must be dropped."""
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(
        ">seq_one\nM\n>seq_two\nMK\n>seq_three\nMKT\n>seq_good\nMKTAY\n",
        encoding="utf-8",
    )
    chunks, skipped = _get_fasta_chunks(fasta, max_seqs=99)
    assert skipped == 2  # seq_one and seq_two are too short
    assert len(chunks) == 1
    assert chunks[0].count(">") == 2  # seq_three (exactly 3) and seq_good survive
    assert "seq_one" not in chunks[0]
    assert "seq_two" not in chunks[0]
    assert "seq_three\n" in chunks[0]


def test_whitespace_only_record_is_skipped(tmp_path):
    """A record whose sequence is only blank lines counts as empty."""
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">seq_blank\n   \n>seq_good\nMKKK\n", encoding="utf-8")
    chunks, skipped = _get_fasta_chunks(fasta, max_seqs=99)
    assert skipped == 1
    assert len(chunks) == 1
    assert chunks[0].count(">") == 1
    assert "seq_blank" not in chunks[0]


def test_all_empty_records_yields_no_chunks(tmp_path):
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">a\n>b\n", encoding="utf-8")
    chunks, skipped = _get_fasta_chunks(fasta, max_seqs=99)
    assert skipped == 2
    assert chunks == []
