from dagster import asset, Field, Config, EnvVar, MetadataValue

import enum
import os
import base64
import math
import tempfile

import polars as pl

from io import BytesIO
from datetime import datetime
from typing import Optional
from Bio import SeqIO, SeqRecord
from Bio.Graphics import GenomeDiagram
from reportlab.lib import colors
from Bio.SeqFeature import SeqFeature, SimpleLocation
from Bio.Graphics.GenomeDiagram import CrossLink
from pathlib import Path
from svgutils import compose as C
from cairosvg import svg2png
from lxml import etree


TEMP_DIR = tempfile.gettempdir()


def gene_uniqueness(
    path_to_dataset: str,
    record_name: list,
) -> pl.DataFrame:
    """Calculate percentage of the presence of a given gene over the displayed sequences"""

    _gene_uniqueness_df = (
        pl.read_parquet(path_to_dataset)
        .filter(
            (pl.col("name").is_in(record_name))
            & (pl.col("source_genome_name").is_in(record_name))
        )
        .with_columns(pl.col("name").n_unique().alias("total_seq"))
        .group_by("name", "gene", "locus_tag", "total_seq")
        .count()
        .with_columns(
            ((pl.col("count") - 1) / (pl.col("total_seq") - 1) * 100).alias(
                "perc_presence"
            )
        )
    )

    return _gene_uniqueness_df


def _assess_file_content(genome: SeqRecord.SeqRecord) -> bool:  # Duplicated function
    """Assess wether the genbank file contains gene or only CDS"""

    _gene_count = 0
    _gene_value = False
    for _feature in genome.features:
        if _feature.type == "gene":
            _gene_count = _gene_count + 1
            if _gene_count > 1:
                _gene_value = True
                break

    return _gene_value


def _get_sqc_identity_from_csv(file_path: str) -> dict:
    _df = pl.read_csv(file_path, has_header=False).select(
        "column_1", pl.col("column_2").cast(pl.Int16)
    )
    return {x: y for x, y in zip(*_df.to_dict(as_series=False).values())}


class CheckOrientation(enum.Enum):
    SEQUENCE = 0
    REVERSE = 1


class Genome(Config):
    sequence_file: str = "sequences.csv"


@asset(
    description="Return a dict from the sequence paths and their orientation.",
    compute_kind="Python",
    metadata={"owner": "Virginie Grosboillot"},
)
def create_genome(context, config: Genome) -> dict:
    # Path to sequence file
    _path_seq = str(
        Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR)) / config.sequence_file
    )
    context.log.info(f"File containing the sequences to plot: {_path_seq}")

    if os.path.exists(_path_seq):
        _sequences = _get_sqc_identity_from_csv(_path_seq)
        for _k, _v in _sequences.items():
            # When the user is lazy and wants to do SEQUENCE=0, or REVERSE=1
            if isinstance(_v, int):
                _sequences[_k] = CheckOrientation(_v).name
    else:
        _sequences = {}
        context.log.info(
            "sequences.csv file not present or the file format is not recognised"
        )

    # Asset metadata
    _time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"Sequences to plot {_time.isoformat()} (UTC).",
            "num_sqcs": len(_sequences),
            "sequences": MetadataValue.json(_sequences),
        }
    )
    return _sequences


def _read_seq(_path: str, _orientation: str) -> SeqRecord.SeqRecord:
    """Read sequence according to genome orientation"""
    if _orientation == CheckOrientation.SEQUENCE.name:
        return SeqIO.read(_path, "gb")
    else:
        return SeqIO.read(_path, "gb").reverse_complement(name=True)


def _get_feature(
    features, id, tags=("locus_tag", "gene", "old_locus_tag", "protein_id")
) -> SeqFeature:
    """Search list of SeqFeature objects for an identifier under the given tags."""
    for _f in features:
        for _key in tags:
            # tag may not be present in this feature
            for _x in _f.qualifiers.get(_key, []):
                if _x == id:  # gene
                    return _f
                elif _x[:-2] == id:  # protein_id
                    return _f
    raise KeyError(id)


class Diagram(Config):
    title: str = "synteny_plot"
    output_format: str = "SVG"
    graph_format: str = "linear"
    graph_pagesize: str = "A4"
    graph_fragments: int = 1
    graph_start: int = 0
    graph_end: Optional[int] = None
    output_folder: str = "synteny"
    blastn_dir: str = str(Path("tables") / "blastn_summary.parquet")
    uniq_dir: str = str(Path("tables") / "uniqueness.parquet")


# gene_uniqueness_folder_config = {
#     "output_folder": Field(
#         str,
#         description="Path to folder where the files will be saved",
#         default_value="table",
#     ),
#     "name": Field(
#         str,
#         description="Path to folder where the files will be saved",
#         default_value="gene_uniqueness",
#     ),
# }


@asset(
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
    context, create_genome: dict, config: Diagram
) -> GenomeDiagram.Diagram:
    # Define the paths
    _gb_folder = str(Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR)) / "genbank")
    _synteny_folder = str(Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR)) / "synteny")
    _blastn_dir = str(
        Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR))
        / "tables"
        / "blastn_summary.parquet"
    )
    _uniq_dir = str(
        Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR)) / "tables" / "uniqueness.parquet"
    )
    _colour_dir = str(Path(_synteny_folder) / "colour_table")

    # Set name for the diagram
    _name_graph = config.title

    # Read sequences for each genome and assign them in a variable
    _records = {}

    for _k, _v in create_genome.items():
        _genbank_path = str(Path(_gb_folder) / _k)
        _record = _read_seq(_genbank_path, _v)
        context.log.info(f"Orientation: {_record}")
        _records[_record.name] = _record

    _record_names = [_rec for _rec in _records.keys()]
    _comparison_tuples = [
        (
            _record_names[_i],
            _record_names[_i + 1],
            f"{_record_names[_i]}_vs_{_record_names[_i+1]}",
        )
        for _i in range(len(_record_names))
        if _i + 1 < len(_record_names)
    ]
    context.log.info(f"List of the records name: {_record_names}")

    # Instanciate the graphic, features, seq_order
    _gd_diagram = GenomeDiagram.Diagram(_name_graph)
    _feature_sets = {}
    _max_len = 0

    context.log.info("Graph has been instantiated")
    _seq_order = {}
    _track_list = [_i for _i in range(1, 2 * len(_records), 2)]

    for _i, (_record_name, _record) in enumerate(_records.items()):
        # Get the longest sequence
        _max_len = max(_max_len, len(_record))
        # Allocate tracks 5 (top), 3, 1 (bottom) for A, B, C
        # (empty tracks 2 and 4 add useful white space to emphasise the cross links
        # and also serve to make the tracks vertically more compressed)
        _gd_track_for_features = _gd_diagram.new_track(
            _track_list[_i],
            name=_record_name,
            greytrack=True,
            greytrack_labels=1,
            greytrack_font_color=colors.black,
            # axis_labels=True,
            height=0.5,
            start=0,
            end=len(_record),
        )
        assert _record_name not in _feature_sets
        _feature_sets[_record_name] = _gd_track_for_features.new_set()
        _seq_order[_record_name] = _i

    context.log.info("Seq order has been determined")

    # We add dummy features to the tracks for each cross-link BEFORE we add the
    # arrow features for the genes. This ensures the genes appear on top:
    for _X, _Y, _X_vs_Y in _comparison_tuples:
        _features_X = _records[_X].features
        _features_Y = _records[_Y].features

        _set_X = _feature_sets[_X]
        _set_Y = _feature_sets[_Y]

        _X_vs_Y = (
            pl.read_parquet(_blastn_dir)
            .filter(
                (pl.col("source_genome_name") == _X)
                & (pl.col("query_genome_name") == _Y)
            )
            .select("source_locus_tag", "query_locus_tag", "percentage_of_identity")
        )

        for _id_X, _id_Y, _perc in _X_vs_Y.iter_rows():
            _color = colors.linearlyInterpolatedColor(
                colors.white, colors.firebrick, 0, 100, _perc
            )
            _border = False
            _f_x = _get_feature(_features_X, _id_X)
            _F_x = _set_X.add_feature(
                SeqFeature(
                    SimpleLocation(_f_x.location.start, _f_x.location.end, strand=0)
                ),
                color=_color,
                border=False,
                flip=True,
            )
            _f_y = _get_feature(_features_Y, _id_Y)
            _F_y = _set_Y.add_feature(
                SeqFeature(
                    SimpleLocation(_f_y.location.start, _f_y.location.end, strand=0)
                ),
                color=_color,
                border=False,
                flip=True,
            )
            _gd_diagram.cross_track_links.append(CrossLink(_F_x, _F_y, _color, _border))

    context.log.info("Cross-links have been appended")

    _gene_color_palette = gene_uniqueness(_uniq_dir, _record_names)
    context.log.info(f"Writing: {str(_colour_dir)}")
    os.makedirs(Path(_colour_dir).parent, exist_ok=True)
    _gene_color_palette.write_parquet(_colour_dir)

    context.log.info("Colour palette has been determined")

    for _record_name, _record in _records.items():
        _gd_feature_set = _feature_sets[_record_name]

        _gene_value = _assess_file_content(_record)
        if _gene_value == True:
            for _feature in _record.features:
                if _feature.type != "gene":
                    # Exclude this feature
                    continue
                try:
                    _perc = (
                        _gene_color_palette.filter(
                            (pl.col("name") == _record_name)
                            & (
                                pl.col("locus_tag")
                                == _feature.qualifiers["locus_tag"][0]
                            )
                        )
                        .select("perc_presence")
                        .item()
                    )
                    if _perc == 0:
                        _gene_color = colors.HexColor(
                            "#fde725"
                        )  # gene_color = colors.HexColor('#440154')
                    elif 0 < _perc <= 20:
                        _gene_color = colors.HexColor(
                            "#90d743"
                        )  # gene_color = colors.HexColor('#443983')
                    elif 20 < _perc <= 40:
                        _gene_color = colors.HexColor(
                            "#35b779"
                        )  # gene_color = colors.HexColor('#31688e')
                    elif 40 < _perc <= 60:
                        _gene_color = colors.HexColor("#21918c")
                    elif 60 < _perc <= 80:
                        _gene_color = colors.HexColor(
                            "#31688e"
                        )  # gene_color = colors.HexColor('#35b779')
                    elif 80 < _perc < 100:
                        _gene_color = colors.HexColor(
                            "#443983"
                        )  # gene_color = colors.HexColor('#90d743')
                    elif _perc == 100:
                        _gene_color = colors.HexColor(
                            "#440154"
                        )  # gene_color = colors.HexColor('#fde725')
                    else:
                        _gene_color = colors.black
                except:
                    _gene_color = colors.white
                try:
                    for _k, _v in _feature.qualifiers.items():
                        if _k == "gene":
                            _name_gene = _v[0]
                except:
                    for _k, _v in _feature.qualifiers.items():
                        if _k == "locus_tag":
                            _name_gene = _v[0]
                finally:
                    _name_gene = ""
                _gd_feature_set.add_feature(
                    _feature,
                    sigil="BIGARROW",
                    color=_gene_color,
                    label=True,
                    name=_name_gene,
                    label_position="middle",
                    label_size=6,
                    label_angle=0,
                    label_strand=1,
                )
        else:
            for _feature in _record.features:
                if _feature.type != "CDS":
                    # Exclude this feature
                    continue
                try:
                    _perc = (
                        _gene_color_palette.filter(
                            (pl.col("name") == _record_name)
                            & (
                                pl.col("locus_tag")
                                == _feature.qualifiers["protein_id"][0][:-2]
                            )
                        )
                        .select("perc_presence")
                        .item()
                    )
                    if _perc == 0:
                        _gene_color = colors.HexColor(
                            "#fde725"
                        )  # gene_color = colors.HexColor('#440154')
                    elif 0 < _perc <= 20:
                        _gene_color = colors.HexColor(
                            "#90d743"
                        )  # gene_color = colors.HexColor('#443983')
                    elif 20 < _perc <= 40:
                        _gene_color = colors.HexColor(
                            "#35b779"
                        )  # gene_color = colors.HexColor('#31688e')
                    elif 40 < _perc <= 60:
                        _gene_color = colors.HexColor("#21918c")
                    elif 60 < _perc <= 80:
                        _gene_color = colors.HexColor(
                            "#31688e"
                        )  # gene_color = colors.HexColor('#35b779')
                    elif 80 < _perc < 100:
                        _gene_color = colors.HexColor(
                            "#443983"
                        )  # gene_color = colors.HexColor('#90d743')
                    elif _perc == 100:
                        _gene_color = colors.HexColor(
                            "#440154"
                        )  # gene_color = colors.HexColor('#fde725')
                    else:
                        _gene_color = colors.black
                except:
                    _gene_color = colors.white
                for _k, _v in _feature.qualifiers.items():
                    if _k == "protein_id":
                        _name_gene = _v[0][:-2]
                _gd_feature_set.add_feature(
                    _feature,
                    sigil="BIGARROW",
                    color=_gene_color,
                    # label=True,
                    # name=_name_gene,
                    # label_position="middle",
                    # label_size=6,
                    # label_angle=0,
                    # label_strand=1,
                )

    context.log.info("Colours have been applied")

    _gd_diagram.draw(
        format=config.graph_format,
        pagesize=config.graph_pagesize,
        fragments=config.graph_fragments,
        start=config.graph_start,
        end=_max_len,
    )

    context.log.info("Graph has been drawn")

    if config.output_format == "SVG":
        _fmt = "svg"
    else:
        _fmt = "png"

    _path_output = str(Path(_synteny_folder) / f"{_name_graph}.{_fmt}")
    _png_output = str(Path(_synteny_folder) / f"{_name_graph}.png")
    _gd_diagram.write(_path_output, config.output_format)

    context.log.info("Parsing SVG xml file")
    tree = etree.parse(_path_output)
    root = tree.getroot()
    width = math.trunc(float(root.attrib.get("width")))
    height = math.trunc(float(root.attrib.get("height")))

    context.log.info(f"W: {width}, H: {height}")

    xpos = int(math.trunc(width * 0.6))
    ypos = int(math.trunc(height * 0.9))
    context.log.info(f"Coord of SVG: {str(xpos)} : {str(ypos)}")

    legend_path = "synphage/assets/viewer/legend.svg"
    # (f"{_synteny_folder}/legend.svg")
    C.Figure(
        f"{width}px",
        f"{height}px",
        C.SVG(_path_output),
        C.SVG(legend_path).scale(10.0).move(xpos, ypos),
    ).save(_path_output)

    svg2png(bytestring=open(_path_output).read(), write_to=_png_output)

    # For metadata
    buffer = BytesIO()
    # _gd_diagram.write(buffer, "png")
    with open(_png_output, "rb") as reader:
        buffer.write(reader.read())

    image_data = base64.b64encode(buffer.getvalue())

    # Asset metadata
    context.add_output_metadata(
        metadata={
            "text_metadata": "A synteny diagram had been created.",
            "num_sqcs": len(_records),
            "path": _path_output,
            "sequences": MetadataValue.json(_record_names),
            "synteny_overview": MetadataValue.md(
                f"![img](data:image/png;base64,{image_data.decode()})"
            ),
        }
    )

    return _gd_diagram
