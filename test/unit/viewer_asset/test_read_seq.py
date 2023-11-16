from Bio.SeqRecord import SeqRecord

from synphage.assets.viewer.viewer import _read_seq


def test_read_seq_sequence():
    result = _read_seq('test/fixtures/viewer/sequences/TT_000001.gb', 'SEQUENCE')
    assert isinstance(result, SeqRecord)
    assert result.seq.startswith('TTAAGATACTTACTACATATCT')
    

def test_read_seq_reverse():
    result = _read_seq('test/fixtures/viewer/sequences/TT_000001.gb', 'REVERSE')
    assert isinstance(result, SeqRecord)
    assert result.seq.startswith('ATTTTGTTAAATTTTCTCA')