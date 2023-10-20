from dagster import asset, Field, Config, EnvVar, MetadataValue

import enum
import hashlib
import os

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Union, Literal, Optional
from pathlib import Path
from Bio import SeqIO, SeqRecord
from Bio.Graphics import GenomeDiagram
from reportlab.lib import colors
from Bio.SeqFeature import SeqFeature, SimpleLocation
from Bio.Graphics.GenomeDiagram import CrossLink
from pyspark.sql import SparkSession

import pyspark.sql.functions as F


# from typing_extension import Literal
from pydantic import Field


def gene_uniqueness(
    spark,
    record_name: list,
    path_to_dataset: str = "/usr/src/data_folder/phage_view_data/gene_identity/gene_uniqueness",
):
    """Calculate percentage of the presence of a given gene over the displayed sequences"""

    gene_uniqueness_df = spark.read.parquet(path_to_dataset).filter(
        (F.col("name").isin(record_name))
        & (F.col("source_genome_name").isin(record_name))
    )
    total_seq = gene_uniqueness_df.select(
        F.count_distinct(F.col("name")).alias("count")
    ).collect()[0][0]
    return (
        gene_uniqueness_df.withColumn("total_seq", F.lit(total_seq))
        .groupby("name", "gene", "locus_tag", "total_seq")
        .count()
        .withColumn(
            "perc_presence", (F.col("count") - 1) / (F.col("total_seq") - 1) * 100
        )
    )


def _assess_file_content(genome) -> bool:  # Duplicated function
    """Assess wether the genbank file contains gene or only CDS"""

    gene_count = 0
    gene_value = False
    for feature in genome.features:
        if feature.type == "gene":
            gene_count = gene_count + 1
            if gene_count > 1:
                gene_value = True
                break

    return gene_value



def _get_sqc_identity_from_csv(file_path):

    spark = SparkSession.builder.getOrCreate()

    df = spark.read.csv(file_path).select('_c0', F.col('_c1').cast("int"))

    sqc_dict = {}
    [sqc_dict.update({x:y}) for x,y in df.toLocalIterator()]

    return sqc_dict


class CheckOrientation(enum.Enum):
    SEQUENCE = 0
    REVERSE = 1


@asset(
    description="Return a dict from the sequence paths and their orientation.",
    compute_kind="Python",
    metadata={"owner": "Virginie Grosboillot"},
)
def create_genome(context):

    context.log.info('get path')
    context.log.info(os.getenv(EnvVar("PHAGY_DIRECTORY")))
    context.log.info(os.getenv(EnvVar("SEQUENCE_FILE")))
    path = '/'.join([os.getenv(EnvVar("PHAGY_DIRECTORY")), os.getenv(EnvVar("SEQUENCE_FILE"))])
    context.log.info(path)

    if os.path.exists(path):
        sequences = _get_sqc_identity_from_csv(path)
    else:
        'The file format is not recognised'

    for k,v in sequences.items():
        # When the user is lazy and wants to do SEQUENCE=0, or REVERSE=1
        if isinstance(v, int):
            sequences[k] = CheckOrientation(v).name

    # Asset metadata
    time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"Dictionnary of sequences to plot {time.isoformat()} (UTC).",
            "num_sqcs": len(sequences),
            "path": path,
            "sequences": MetadataValue.json(sequences),
        }
    )
    return sequences

    
# @dataclass
# class Genome:
#     path: str
#     orientation: CheckOrientation

#     @property
#     def key(self):
#         return (
#             hashlib.blake2s(
#                 bytes(
#                     f"{self.path}{self.orientation}",
#                     "utf-8",
#                 )
#             )
#             .hexdigest()
#             .upper()
#         )

#     def __post_init__(self):
#         if isinstance(self.orientation, int):
#             # When the user is lazy and wants to do SEQUENCE=0, or REVERSE=1
#             self.orientation = CheckOrientation(self.orientation)

#     def __repr__(self):
#         return f"file: {Path(self.path).stem}, orientation: {self.orientation}"

#     def __rshift__(self, genome_dict: Dict[str, Any]) -> Dict[str, Any]:
#         genome_dict[self.key] = self
#         return genome_dict


# class Diagram:
#     def __init__(self):
#         self._genome: Dict[str:Genome] = {}
#         self.rows = -1

#     def __repr__(self):
#         return f"Diagram(genomes:{self.sum})"

#     @property
#     def sum(self):
#         """Collect of genomes"""
#         return len(self._genome)

#     @property
#     def empty(self):
#         """True when no rules are added in the check"""
#         return len(self.rules) == 0

#     def add_genome(self, path: str, orientation: int = 0):
#         """Add a new genome in the Diagram class"""
#         Genome(path, orientation) >> self._genome
#         return self


# synteny_folder_config = {
#     "synteny_directory": Field(
#         str,
#         description="Path to folder containing the data for synteny visualisation",
#         default_value="/usr/src/data_folder/phage_view_data/synteny",
#     ),
# }





def _read_seq(path: str, orientation: str) -> SeqRecord.SeqRecord:
    """Read sequence according to genome orientation"""
    if orientation == CheckOrientation.SEQUENCE.name:
        return SeqIO.read(path, "gb")
    else:
        return SeqIO.read(path, "gb").reverse_complement(name=True)


def _get_feature(
    features, id, tags=("locus_tag", "gene", "old_locus_tag", "protein_id")
):
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


# class Genome(Config):
#     genomes: Dict[str, CheckOrientation]
    # genomes: Dict[str = Field(description= "Path to the genome"), CheckOrientation = Field(description= "For displaying the sequence in the rigth orientation")]


#     # @property
#     # def key(self):
#     #     return (
#     #         hashlib.blake2s(
#     #             bytes(
#     #                 f"{self.path}{self.orientation}",
#     #                 "utf-8",
#     #             )
#     #         )
#     #         .hexdigest()
#     #         .upper()
#     #     )

#     # def __post_init__(self):
#     #     if isinstance(self.orientation, int):
#     #         # When the user is lazy and wants to do SEQUENCE=0, or REVERSE=1
#     #         self.orientation = CheckOrientation(self.orientation)

#     # def __repr__(self):
#     #     return f"file: {Path(self.path).stem}, orientation: {self.orientation}"

#     # def __rshift__(self, genome_dict: Dict[str, Any]) -> Dict[str, Any]:
#     #     genome_dict[self.key] = self
#     #     return genome_dict


class Diagram(Config):
    title: str = "diagram"
    output_format: str = "SVG"
    graph_format: str = "linear"
    graph_pagesize: str = "A4"
    graph_fragments: int = 1
    graph_start: int = 0
    graph_end: Optional[int] = None
    output_folder: str = "synteny"
    blastn_dir: str = (
        "blastn_summary"
    )
    uniq_dir: str = "gene_uniqueness"


#     # def __repr__(self):
#     #     return f"Diagram(genomes:{self.sum})"

#     # @property
#     # def sum(self):
#     #     """Collect of genomes"""
#     #     return len(self._genome)

#     # @property
#     # def empty(self):
#     #     """True when no rules are added in the check"""
#     #     return len(self.rules) == 0

#     # def add_genome(self, path: str, orientation: int = 0):
#     #     """Add a new genome in the Diagram class"""
#     #     Genome(path, orientation) >> self._genome
#     #     return self
gene_uniqueness_folder_config = {
    "output_folder": Field(
        str,
        description="Path to folder where the files will be saved",
        default_value="table",
    ),
    "name": Field(
        str,
        description="Path to folder where the files will be saved",
        default_value="gene_uniqueness",
    ),
}

@asset(
    #config_schema={**gene_uniqueness_folder_config},
    description="Transform a list of genomes into a genome diagram",
    compute_kind="Biopython",
    metadata={
                "tables": "table",
                "name": "blastn_summary",
                "name2": "gene_uniqueness",
                "parquet_managment": "append",
                "owner": "Virginie Grosboillot",
            },
)
def create_graph(
    context, create_genome, extract_locus_tag_gene, parse_blastn, config: Diagram
):  # parse_blastn

    output_folder= "/".join([os.getenv(EnvVar("PHAGY_DIRECTORY")), "synteny"])
    blastn_dir= "/".join([os.getenv(EnvVar("PHAGY_DIRECTORY")), "table", "blastn_summary"])
    uniq_dir= "/".join([os.getenv(EnvVar("PHAGY_DIRECTORY")), "table", "gene_uniqueness"])
    colour_dir = "/".join([output_folder, "colour_table"])

    # Initiate SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Set name for the diagram
    name = config.title

    # Read sequences for each genome and assign them in a variable
    records = {}

    for k, v in create_genome.items():
        record = _read_seq(k, v)
        context.log.info(f"Orientation: {record}")
        #context.log.info(f"Orientation: {orientation}")
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
    context.log.info(f"List of the records name: {record_names}")

    # instanciate the graphic, features, seq_order
    gd_diagram = GenomeDiagram.Diagram(name)
    feature_sets = {}
    max_len = 0

    context.log.info("Graph has been instantiated")
    seq_order = {}
    track_list = [i for i in range(1, 2 * len(records), 2)]

    for i, (record_name, record) in enumerate(records.items()):
        # Get the longest sequence
        max_len = max(max_len, len(record))
        # Allocate tracks 5 (top), 3, 1 (bottom) for A, B, C
        # (empty tracks 2 and 4 add useful white space to emphasise the cross links
        # and also serve to make the tracks vertically more compressed)
        gd_track_for_features = gd_diagram.new_track(
            track_list[i],
            name=record_name,
            greytrack=True,
            greytrack_labels=1,
            greytrack_font_color=colors.black,
            # axis_labels=True,
            height=0.5,
            start=0,
            end=len(record),
        )
        assert record_name not in feature_sets
        feature_sets[record_name] = gd_track_for_features.new_set()
        seq_order[record_name] = i
    
    context.log.info("Seq order has been determined")

    # We add dummy features to the tracks for each cross-link BEFORE we add the
    # arrow features for the genes. This ensures the genes appear on top:
    for X, Y, X_vs_Y in comparison_tuples:
        features_X = records[X].features
        features_Y = records[Y].features

        set_X = feature_sets[X]
        set_Y = feature_sets[Y]

        X_vs_Y = (
            spark.read.parquet(blastn_dir)
            .filter(
                (F.col("source_genome_name") == X) & (F.col("query_genome_name") == Y)
            )
            .select("source_locus_tag", "query_locus_tag", "percentage_of_identity")
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

    context.log.info("Cross-links have been appended")

    gene_color_palette = gene_uniqueness(spark, record_names, uniq_dir)
    gene_color_palette.write.mode('overwrite').parquet(colour_dir)

    context.log.info("Colour palette has been determined")

    for record_name, record in records.items():
        gd_feature_set = feature_sets[record_name]

        gene_value = _assess_file_content(record)
        if gene_value == True:
            for feature in record.features:
                if feature.type != "gene":
                    # Exclude this feature
                    continue
                try:
                    perc = (
                        gene_color_palette.filter(
                            (F.col("name") == record_name)
                            & (F.col("locus_tag") == feature.qualifiers["locus_tag"][0])
                        )
                        .select("perc_presence")
                        .collect()[0][0]
                    )
                    if perc == 0:
                        gene_color = colors.HexColor(
                            "#fde725"
                        )  # gene_color = colors.HexColor('#440154')
                    elif 0 < perc <= 20:
                        gene_color = colors.HexColor(
                            "#90d743"
                        )  # gene_color = colors.HexColor('#443983')
                    elif 20 < perc <= 40:
                        gene_color = colors.HexColor(
                            "#35b779"
                        )  # gene_color = colors.HexColor('#31688e')
                    elif 40 < perc <= 60:
                        gene_color = colors.HexColor("#21918c")
                    elif 60 < perc <= 80:
                        gene_color = colors.HexColor(
                            "#31688e"
                        )  # gene_color = colors.HexColor('#35b779')
                    elif 80 < perc < 100:
                        gene_color = colors.HexColor(
                            "#443983"
                        )  # gene_color = colors.HexColor('#90d743')
                    elif perc == 100:
                        gene_color = colors.HexColor(
                            "#440154"
                        )  # gene_color = colors.HexColor('#fde725')
                    else:
                        gene_color = colors.black
                except:
                    gene_color = colors.white
                try:
                    for k, v in feature.qualifiers.items():
                        if k == "gene":
                            name = v[0]
                except:
                    for k, v in feature.qualifiers.items():
                        if k == "locus_tag":
                            name = v[0]
                finally:
                    name = ""
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
                    perc = (
                        gene_color_palette.filter(
                            (F.col("name") == record_name)
                            & (
                                F.col("locus_tag")
                                == feature.qualifiers["protein_id"][0][:-2]
                            )
                        )
                        .select("perc_presence")
                        .collect()[0][0]
                    )
                    if perc == 0:
                        gene_color = colors.HexColor(
                            "#fde725"
                        )  # gene_color = colors.HexColor('#440154')
                    elif 0 < perc <= 20:
                        gene_color = colors.HexColor(
                            "#90d743"
                        )  # gene_color = colors.HexColor('#443983')
                    elif 20 < perc <= 40:
                        gene_color = colors.HexColor(
                            "#35b779"
                        )  # gene_color = colors.HexColor('#31688e')
                    elif 40 < perc <= 60:
                        gene_color = colors.HexColor("#21918c")
                    elif 60 < perc <= 80:
                        gene_color = colors.HexColor(
                            "#31688e"
                        )  # gene_color = colors.HexColor('#35b779')
                    elif 80 < perc < 100:
                        gene_color = colors.HexColor(
                            "#443983"
                        )  # gene_color = colors.HexColor('#90d743')
                    elif perc == 100:
                        gene_color = colors.HexColor(
                            "#440154"
                        )  # gene_color = colors.HexColor('#fde725')
                    else:
                        gene_color = colors.black
                except:
                    gene_color = colors.white
                for k, v in feature.qualifiers.items():
                    if k == "protein_id":
                        name = v[0][:-2]
                gd_feature_set.add_feature(
                    feature,
                    sigil="BIGARROW",
                    color=gene_color,
                    # label=True,
                    # name=name,
                    # label_position="middle",
                    # label_size=6,
                    # label_angle=0,
                    # label_strand=1,
                )

    context.log.info("Colours have been applied")

    if isinstance(config.graph_end, int):
        graph_end = config.graph_end
    else:
        graph_end = max_len

    gd_diagram.draw(
        format=config.graph_format,
        pagesize=config.graph_pagesize,
        fragments=config.graph_fragments,
        start=config.graph_start,
        end=graph_end,
    )

    context.log.info("Graph has been drawn")

    if config.output_format == "SVG":
        fmt = "svg"
    else:
        fmt = "png"

    path_output = f"{output_folder}/{name}.{fmt}"
    # return gd_diagram.write(f"{config.output_folder}/{name}.{fmt}", config.output_format)
    return gd_diagram.write(path_output, config.output_format)


#         self,
#         spark,
#         folder: str,
#         blastn_path: str,
#         gene_uniq_path: str,
#         graph_title: str = "Title:",
#     ):
#         """Transform a list of genomes into a genome diagram"""


#         # import numpy as np
#         # transcriptomic_data =  list(np.random.uniform(low=5.0, high=18.0, size=(203, )))
#         # transcriptomic = {'trans_name', transcriptomic_data}

#         # nbr_add_tracks = len(transcriptomic)
#         # graphTracks = [i for i in range(2*len(records)-1, nbr_add_tracks, 1)]

#         # transcriptomic_sets = {}

#         # for i, cond in zip(graphTracks, transcriptomic.keys()):
#         #     gd_track_for_transcriptomic = gd_diagram.new_track(
#         #         i,
#         #         name=cond,
#         #         greytrack=True,
#         #         height=0.5,
#         #         start=0,
#         #         end=len(records['']),
#         #     )
#         #     assert cond not in transcriptomic_sets
#         #     transcriptomic_sets[cond] = gd_track_for_transcriptomic.new_set()
#         #     seq_order[cond] = i
#         # for cond, data in transcriptomic.items():
#         #     gd_transcriptomic_set = transcriptomic_sets[cond]
#         #     gd_transcriptomic_set.new_graph(data, f'{cond}', style='line', colour=colors.violet) #'bar'
