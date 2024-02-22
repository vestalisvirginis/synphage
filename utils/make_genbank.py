from Bio import SeqIO


def convert(x):
    """Format DNA seq for gb file"""
    index = 0
    position = 1
    pad = int(str(len(x)))
    print(str(position).rjust(pad, " "), end=" ")
    for a,b in zip(range(0,len(x)+1, 10), range(10, len(x)+1, 10)):
        print(f"{x[a:b]} ", end="")
        index += 1
        if (index % 6) == 0:
            index = 0
            print("")
            print(str((position * 60) + 1).rjust(pad, " "), end=" ")
            position += 1

            

if __name__ == "__main__":
    s = SeqIO.read("test/fixtures/viewer/sequences/TT_000001.gb", "gb")
    x = str(s.seq) 
    convert(x)