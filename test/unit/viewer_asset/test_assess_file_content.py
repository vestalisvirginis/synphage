from Bio import SeqIO

from synphage.assets.blaster.blaster import _assess_file_content


def test_contains_genes_pos():
    _genome = SeqIO.read("test/fixtures/blaster/gene_or_cds/TT_000001.gb", "genbank")
    result = _assess_file_content(_genome)
    assert result == True


def test_contains_genes_neg():
    _genome = SeqIO.read("test/fixtures/blaster/gene_or_cds/TT_000006.gb", "genbank")
    result = _assess_file_content(_genome)
    assert result == False
