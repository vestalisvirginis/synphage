from reportlab.lib import colors
from reportlab.lib.units import cm
from Bio.Graphics import GenomeDiagram
from Bio import SeqIO

record = SeqIO.read("./tests/fixtures/phage_sequences/168_SPbeta.gb", "genbank")

gd_diagram = GenomeDiagram.Diagram("Bacillus subtillis SPbeta")
gd_track_for_features = gd_diagram.new_track(1, name="Annotated Features")
gd_feature_set = gd_track_for_features.new_set()

for feature in record.features:
    if feature.type != "gene":
        # Exclude this feature
        continue
    if len(gd_feature_set) % 2 == 0:
        color = colors.blue
    else:
        color = colors.lightblue
    gd_feature_set.add_feature(
        feature,
        sigil="BIGARROW",
        color=color,
        label=True,
        label_position="end",
        label_size=10,
        label_angle=90,
        )


gd_diagram.draw(
    format="linear",
    orientation="landscape",
    pagesize="A4",
    fragments=4,
    start=0,
    end=len(record),
)

gd_diagram.write("plasmid_linear.pdf", "PDF")




# Several genes
A_rec = SeqIO.read("./tests/fixtures/phage_sequences/168_SPbeta.gb", "gb")
B_rec = SeqIO.read("./tests/fixtures/phage_sequences/phage_Goe11.gb", "gb")
C_rec = SeqIO.read("./tests/fixtures/phage_sequences/phage_Goe14.gb", "gb")

from reportlab.lib.colors import (
    red,
    grey,
    orange,
    green,
    brown,
    blue,
    lightblue,
    purple,
)

A_colors = (
    [red] * 5
    + [grey] * 7
    + [orange] * 2
    + [grey] * 2
    + [orange]
    + [grey] * 11
    + [green] * 4
    + [grey]
    + [green] * 2
    + [grey, green]
    + [brown] * 5
    + [blue] * 4
    + [lightblue] * 5
    + [grey, lightblue]
    + [purple] * 2
    + [grey]
)
B_colors = (
    [red] * 6
    + [grey] * 8
    + [orange] * 2
    + [grey]
    + [orange]
    + [grey] * 21
    + [green] * 5
    + [grey]
    + [brown] * 4
    + [blue] * 3
    + [lightblue] * 3
    + [grey] * 5
    + [purple] * 2
)
C_colors = (
    [grey] * 30
    + [green] * 5
    + [brown] * 4
    + [blue] * 2
    + [grey, blue]
    + [lightblue] * 2
    + [grey] * 5
)

from Bio.Graphics import GenomeDiagram

name = "Proux Fig 6"
gd_diagram = GenomeDiagram.Diagram(name)
max_len = 0
for record, gene_colors in zip([A_rec, B_rec, C_rec], [A_colors, B_colors, C_colors]):
    max_len = max(max_len, len(record))
    gd_track_for_features = gd_diagram.new_track(
        1, name=record.name, greytrack=True, start=0, end=len(record)
    )
    gd_feature_set = gd_track_for_features.new_set()

    i = 0
    for feature in record.features:
        if feature.type != "gene":
            # Exclude this feature
            continue
        gd_feature_set.add_feature(
            feature,
            sigil="ARROW",
            color=gene_colors[i],
            label=True,
            name=str(i + 1),
            label_position="start",
            label_size=6,
            label_angle=0,
        )
        i += 1

gd_diagram.draw(format="linear", pagesize="A4", fragments=1, start=0, end=max_len)
gd_diagram.write(name + ".pdf", "PDF")
gd_diagram.write(name + ".eps", "EPS")
gd_diagram.write(name + ".svg", "SVG")


import os

name = 'phage_Goe11'
path = f'tests/fixtures/phage_sequences/{name}.gb'


from Bio import SeqIO

for seq_record in SeqIO.parse(path, 'genbank'):
     print("Dealing with GenBank record %s" % seq_record.id)
     open(f"tests/fixtures/converted_to_fasta/{name}_converted.fna", "w").write(">%s %s\n%s\n" % (
            seq_record.id,
            seq_record.description,
            seq_record.seq))

output_handle.close()

os.system(f"makeblastdb -in tests/fixtures/converted_to_fasta/{name}.fna -input_type fasta -dbtype nucl -out tests/fixtures/database/{name}_converted")

os.system(f"blastn -query tests/fixtures/converted_to_fasta/{name}_converted.fna -db tests/fixtures/database/{name}_converted -evalue 1e-3 -dust no -out tests/fixtures/blastn_results/{name}_converted.bls")


genome = SeqIO.read('tests/fixtures/phage_sequences/168_SPbeta.gb', "genbank")

for feature in genome.features:
    if feature.type =="gene":
        if 'gene' in feature.qualifiers.keys():
            print(feature.qualifiers['gene'][0])
            print(feature.qualifiers['locus_tag'][0])
            print(feature.location)
        else:
            print('None')
            print(feature.qualifiers['locus_tag'][0])
            print(feature.location)


for seq_record in SeqIO.parse(path, 'genbank'):
    print(str(seq_record.seq)[:10])
    print(str(seq_record.seq)[11:20])



with open ("tests/fixtures/converted_to_fasta/test_2_converted.fna", "w") as f:
    for feature in genome.features:
        if feature.type =="gene":
            for seq_record in SeqIO.parse(path, 'genbank'):
                f.write(">%s %s %s %s %s\n%s\n" % (
                    seq_record.id,
                    seq_record.description,
                    feature.qualifiers['gene'][0] if 'gene' in feature.qualifiers.keys() else 'None',
                    feature.qualifiers['locus_tag'][0],
                    feature.location,
                    seq_record.seq[feature.location.start:feature.location.end]
                ))
