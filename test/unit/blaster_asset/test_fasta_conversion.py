import pytest

from pathlib import PosixPath
from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.blaster.blaster import genbank_to_fasta


def test_genbank_to_fasta(mock_env_phagy_dir):
    context = build_asset_context()
    asset_input = [PosixPath('test/fixtures/synthetic_data/genbank/TT_000001.gb'), PosixPath('test/fixtures/synthetic_data/genbank/TT_000002.gb'), PosixPath('test/fixtures/synthetic_data/genbank/TT_000003.gb'), PosixPath('test/fixtures/synthetic_data/genbank/TT_000004.gb'), PosixPath('test/fixtures/synthetic_data/genbank/TT_000005.gb')]
    result = genbank_to_fasta(context, asset_input)
    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert len(result[0]) == 5
    assert set(result[0]) == set(['test/fixtures/synthetic_data/gene_identity/fasta/TT_000001.fna', 'test/fixtures/synthetic_data/gene_identity/fasta/TT_000002.fna', 'test/fixtures/synthetic_data/gene_identity/fasta/TT_000003.fna', 'test/fixtures/synthetic_data/gene_identity/fasta/TT_000004.fna', 'test/fixtures/synthetic_data/gene_identity/fasta/TT_000005.fna'])
    assert isinstance(result[1], list)
    assert len(result[1]) == 5
    assert set(result[1]) == set(['TT_000001', 'TT_000002', 'TT_000003', 'TT_000004', 'TT_000005'])


def test_genbank_to_fasta_assets():
    @asset(name="standardised_ext_file")
    def mock_upstream():
        return  [PosixPath('test/fixtures/synthetic_data/genbank/TT_000001.gb'), PosixPath('test/fixtures/synthetic_data/genbank/TT_000002.gb'), PosixPath('test/fixtures/synthetic_data/genbank/TT_000003.gb'), PosixPath('test/fixtures/synthetic_data/genbank/TT_000004.gb'), PosixPath('test/fixtures/synthetic_data/genbank/TT_000005.gb')]
    assets = [genbank_to_fasta, mock_upstream]
    result = materialize_to_memory(assets)
    assert result.success
    new_fasta_files = result.output_for_node("genbank_to_fasta", "new_fasta_files")
    all_fasta_files = result.output_for_node("genbank_to_fasta", "history_fasta_files")
    assert len(new_fasta_files) == 5
    assert len(all_fasta_files) == 5


@pytest.mark.skip
def test_file_conversion_positive_no_output():
    rs = BLT.genbank_to_fasta("tests/fixtures/synthetic_data/genbank/TT_000001.gb")
    assert rs.endswith("TT_000001.fna has been written.")
    assert [
        row for row in open("tests/fixtures/synthetic_data/genbank/TT_000001.fna")
    ] == [row for row in open("tests/fixtures/synthetic_data/fasta/TT_000001.fna")]


@pytest.mark.skip
def test_file_conversion_positive_w_output(tmp_path):
    d = tmp_path / "fasta"
    d.mkdir()
    p = f"{d}/TT_000001.fna"
    rs = BLT.genbank_to_fasta("tests/fixtures/synthetic_data/genbank/TT_000001.gb", p)
    assert isinstance(rs, str)
    assert rs.endswith(f"File {p} has been written.")


@pytest.mark.skip
def test_file_conversion_negative():
    # empty file
    # error in file
    # wrong format (not genbank')
    pass