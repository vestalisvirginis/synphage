import pytest
from Bio import SeqIO

from synphage.assets.blaster.n_blaster_old import _assess_file_content


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_contains_genes_pos():
    _genome = SeqIO.read("test/fixtures/blaster/gene_or_cds/TT_000001.gb", "genbank")
    result = _assess_file_content(_genome)
    assert result == True


@pytest.mark.skip(reason="need to rewrite test to accomodate changes")
def test_contains_genes_neg():
    _genome = SeqIO.read("test/fixtures/blaster/gene_or_cds/TT_000006.gb", "genbank")
    result = _assess_file_content(_genome)
    assert result == False
