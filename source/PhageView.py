import enum
import hashlib

from dataclasses import dataclass
from typing import Dict, Any
from pathlib import Path
from Bio import SeqIO, SeqRecord
from Bio.Graphics import GenomeDiagram
from reportlab.lib import colors
from Bio.SeqFeature import SeqFeature, SimpleLocation
from Bio.Graphics.GenomeDiagram import CrossLink

import pyspark.sql.functions as F


from source.Blaster import gene_uniqueness, assess_file_content


class CheckOrientation(enum.Enum):
    SEQUENCE = 0
    REVERSE = 1


def _read_seq(path: str, orientation: CheckOrientation) -> SeqRecord.SeqRecord:
    """Read sequence according to genome orientation"""
    if orientation.name == CheckOrientation.SEQUENCE.name:
        return SeqIO.read(path, "gb")
    else:
        return SeqIO.read(path, "gb").reverse_complement(name=True)


def _get_feature(features, id, tags=("locus_tag", "gene", "old_locus_tag", "protein_id")):
    """Search list of SeqFeature objects for an identifier under the given tags."""
    for f in features:
        for key in tags:
            # tag may not be present in this feature
            for x in f.qualifiers.get(key, []):
                if x == id:
                    return f
                elif x[:-2] == id:
                    return f
    raise KeyError(id)


@dataclass
class Genome:
    path: str
    orientation: CheckOrientation

    @property
    def key(self):
        return (
            hashlib.blake2s(
                bytes(
                    f"{self.path}{self.orientation}",
                    "utf-8",
                )
            )
            .hexdigest()
            .upper()
        )

    def __post_init__(self):
        if isinstance(self.orientation, int):
            # When the user is lazy and wants to do SEQUENCE=0, or REVERSE=1
            self.orientation = CheckOrientation(self.orientation)

    def __repr__(self):
        return f"file: {Path(self.path).stem}, orientation: {self.orientation}"

    def __rshift__(self, genome_dict: Dict[str, Any]) -> Dict[str, Any]:
        genome_dict[self.key] = self
        return genome_dict


class Diagram:
    def __init__(self):
        self._genome: Dict[str:Genome] = {}
        self.rows = -1

    def __repr__(self):
        return f"Diagram(genomes:{self.sum})"

    @property
    def sum(self):
        """Collect of genomes"""
        return len(self._genome)

    @property
    def empty(self):
        """True when no rules are added in the check"""
        return len(self.rules) == 0

    def add_genome(self, path: str, orientation: int = 0):
        """Add a new genome in the Diagram class"""
        Genome(path, orientation) >> self._genome
        return self

    def create_graph(
        self,
        spark,
        folder: str,
        blastn_path: str,
        gene_uniq_path: str,
        graph_title: str = "Title:",
    ):
        """Transform a list of genomes into a genome diagram"""

        # Set name for the diagram
        name = graph_title

        # Read sequences for each genome and assign them in a variable
        records = {}

        for v in self._genome.values():
            record = _read_seq(v.path, v.orientation)
            records[record.name] = record

        record_names = [rec for rec in records.keys()]
        comparison_tuples = [
            (
                record_names[i],
                record_names[i + 1],
                f"{record_names[i]}_vs_{record_names[i+1]}",
            )
            for i in range(len(record_names))
            if i + 1 < len(record_names)
        ]

        # records = {rec.name: rec for rec in sequences}

        gd_diagram = GenomeDiagram.Diagram(name)
        feature_sets = {}
        max_len = 0

        seq_order = {}
        # track_list = [i for i in range(1, 2*len(records), 2)]

        for i, (record_name, record) in enumerate(records.items(), 1):
            # Get the longest sequence
            max_len = max(max_len, len(record))
            # Allocate tracks 5 (top), 3, 1 (bottom) for A, B, C
            # (empty tracks 2 and 4 add useful white space to emphasise the cross links
            # and also serve to make the tracks vertically more compressed)
            gd_track_for_features = gd_diagram.new_track(
                i, #2 * i - 1,
                name=record_name,
                greytrack=True,
                greytrack_labels=1,
                greytrack_font_color=colors.black,
                #axis_labels=True,
                height=0.5,
                start=0,
                end=len(record),
            )
            assert record_name not in feature_sets
            feature_sets[record_name] = gd_track_for_features.new_set()
            seq_order[record_name] = i

        

        # We add dummy features to the tracks for each cross-link BEFORE we add the
        # arrow features for the genes. This ensures the genes appear on top:
        for X, Y, X_vs_Y in comparison_tuples:
            features_X = records[X].features
            features_Y = records[Y].features

            set_X = feature_sets[X]
            set_Y = feature_sets[Y]

            X_vs_Y = spark.read.parquet(blastn_path).filter((F.col('source_genome_name')==X) & (F.col('query_genome_name')==Y)).select(
                "source_locus_tag", "query_locus_tag", "percentage_of_identity"
            )

            for id_X, id_Y, perc in X_vs_Y.toLocalIterator():
                color = colors.linearlyInterpolatedColor(
                    colors.white, colors.firebrick, 0, 100, perc
                )
                border = False
                f_x = _get_feature(features_X, id_X)
                F_x = set_X.add_feature(
                    SeqFeature(
                        SimpleLocation(f_x.location.start, f_x.location.end, strand=0)
                    ), 
                    color=color,
                    border=False,
                    flip=True,
                )
                f_y = _get_feature(features_Y, id_Y)
                F_y = set_Y.add_feature(
                    SeqFeature(
                        SimpleLocation(f_y.location.start, f_y.location.end, strand=0)
                    ),
                    color=color,
                    border=False,
                    flip=True,
                )
                gd_diagram.cross_track_links.append(CrossLink(F_x, F_y, color, border))

        gene_color_palette = gene_uniqueness(spark, record_names, gene_uniq_path)
        for record_name, record in records.items():
            gd_feature_set = feature_sets[record_name]

            gene_value = assess_file_content(record)
            if gene_value == True:
                for feature in record.features:
                    if feature.type != "gene":
                        # Exclude this feature
                        continue
                    try: 
                        perc = gene_color_palette.filter(
                                (F.col("name") == record_name)
                                & (F.col("locus_tag") == feature.qualifiers["locus_tag"][0])
                            ).select("perc_presence").collect()[0][0]
                        if perc == 0:
                            gene_color = colors.HexColor('#fde725')      #gene_color = colors.HexColor('#440154')
                        elif 0 < perc <= 20:
                            gene_color = colors.HexColor('#90d743')      #gene_color = colors.HexColor('#443983')
                        elif 20 < perc <= 40:
                            gene_color = colors.HexColor('#35b779')      #gene_color = colors.HexColor('#31688e')
                        elif 40 < perc <= 60:
                            gene_color = colors.HexColor('#21918c')
                        elif 60 < perc <= 80:
                            gene_color = colors.HexColor('#31688e')       #gene_color = colors.HexColor('#35b779')
                        elif 80 < perc < 100:
                            gene_color = colors.HexColor('#443983')       #gene_color = colors.HexColor('#90d743')
                        elif perc == 100:
                            gene_color = colors.HexColor('#440154')        #gene_color = colors.HexColor('#fde725')
                        else:
                            gene_color = colors.black
                    except:
                        gene_color = colors.white
                    try:
                        for k,v in feature.qualifiers.items():
                            if k == 'gene':
                                name = v[0]
                    except:
                        for k,v in feature.qualifiers.items():
                            if k == 'locus_tag':
                                name = v[0]
                    finally:
                        name=''
                    gd_feature_set.add_feature(
                        feature,
                        sigil="BIGARROW",
                        color=gene_color,
                        label=True,
                        name=name,
                        label_position="middle",
                        label_size=6,
                        label_angle=0,
                        label_strand=1,
                    )
            else:
                for feature in record.features:
                    if feature.type != "CDS":
                        # Exclude this feature
                        continue
                    try: 
                        perc = gene_color_palette.filter(
                                (F.col("name") == record_name)
                                & (F.col("locus_tag") == feature.qualifiers["protein_id"][0][:-2])
                            ).select("perc_presence").collect()[0][0]
                        if perc == 0:
                            gene_color = colors.HexColor('#fde725')      #gene_color = colors.HexColor('#440154')
                        elif 0 < perc <= 20:
                            gene_color = colors.HexColor('#90d743')      #gene_color = colors.HexColor('#443983')
                        elif 20 < perc <= 40:
                            gene_color = colors.HexColor('#35b779')      #gene_color = colors.HexColor('#31688e')
                        elif 40 < perc <= 60:
                            gene_color = colors.HexColor('#21918c')
                        elif 60 < perc <= 80:
                            gene_color = colors.HexColor('#31688e')       #gene_color = colors.HexColor('#35b779')
                        elif 80 < perc < 100:
                            gene_color = colors.HexColor('#443983')       #gene_color = colors.HexColor('#90d743')
                        elif perc == 100:
                            gene_color = colors.HexColor('#440154')        #gene_color = colors.HexColor('#fde725')
                        else:
                            gene_color = colors.black
                    except:
                        gene_color = colors.white
                    for k,v in feature.qualifiers.items():
                        if k == 'protein_id':
                            name = v[0][:-2]
                    gd_feature_set.add_feature(
                        feature,
                        sigil="BIGARROW",
                        color=gene_color,
                        #label=True,
                        #name=name,
                        #label_position="middle",
                        #label_size=6,
                        #label_angle=0,
                        #label_strand=1,
                    )
        # import numpy as np
        # transcriptomic_data =  list(np.random.uniform(low=5.0, high=18.0, size=(203, )))
        # transcriptomic = {'trans_name', transcriptomic_data}

        # nbr_add_tracks = len(transcriptomic)
        # graphTracks = [i for i in range(2*len(records)-1, nbr_add_tracks, 1)]

        # transcriptomic_sets = {}

        # for i, cond in zip(graphTracks, transcriptomic.keys()):
        #     gd_track_for_transcriptomic = gd_diagram.new_track(
        #         i,
        #         name=cond,
        #         greytrack=True,
        #         height=0.5,
        #         start=0,
        #         end=len(records['']),
        #     )
        #     assert cond not in transcriptomic_sets
        #     transcriptomic_sets[cond] = gd_track_for_transcriptomic.new_set()
        #     seq_order[cond] = i
        # for cond, data in transcriptomic.items():
        #     gd_transcriptomic_set = transcriptomic_sets[cond]
        #     gd_transcriptomic_set.new_graph(data, f'{cond}', style='line', colour=colors.violet) #'bar'

        gd_diagram.draw(
            format="linear", pagesize="A4", fragments=1, start=0, end=max_len
        )
        gd_diagram.write(f"{folder}/{name}.svg", "SVG")
