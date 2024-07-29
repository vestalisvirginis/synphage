from dagster import asset, Config, MetadataValue, AssetObservation, AssetSpec

import enum
import os
import base64
import math
import tempfile

import polars as pl

from pydantic import Field
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
from string import Template
from PIL import ImageColor
from synphage.resources.local_resource import OWNER


TEMP_DIR = tempfile.gettempdir()


def gene_uniqueness(
    path_to_dataset: str,
    record_name: list,
) -> pl.DataFrame:
    """Calculate percentage of the presence of a given gene over the displayed sequences"""

    _gene_uniqueness_df = (
        pl.read_parquet(path_to_dataset)
        .filter(
            (pl.col("query_name").is_in(record_name))
            & (
                (pl.col("source_name").is_in(record_name))
                | (pl.col("source_name")).is_null()
            )
        )
        .with_columns(
            pl.col("query_name").n_unique().alias("total_seq"),
            pl.when(pl.col("source_name").is_null())
            .then(pl.lit(0))
            .otherwise(pl.lit(1))
            .alias("counter_start"),
        )
        .group_by(
            "query_name", "query_gene", "query_locus_tag", "total_seq", "counter_start"
        )
        .len()
        .with_columns((pl.col("len") + pl.col("counter_start") - 1).alias("count"))
        .with_columns(
            (pl.col("count") / (pl.col("total_seq") - 1) * 100).alias("perc_presence")
        )
    )

    return _gene_uniqueness_df


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
    deps=[
        AssetSpec("transform_blastn", skippable=True),
        AssetSpec("transform_blastp", skippable=True),
    ],
    description="Load file names and orientations for the sequences to be plotted",
    required_resource_keys={"local_resource"},
    compute_kind="Python",
    metadata={"owner": OWNER},
)
def create_genome(context, config: Genome) -> dict:
    # Path to sequence file
    _path_seq = str(
        Path(context.resources.local_resource.get_paths()["SYNPHAGE_DATA"])
        / config.sequence_file
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
        context.log_event(
            AssetObservation(
                asset_key="create_genome",
                metadata={
                    "message": f"{config.sequence_file} file not present or the file format is not recognised"
                },
            )
        )

    # Asset metadata
    context.add_output_metadata(
        metadata={
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
    graph_type: str = Field(
        default="blastn",
        description="Data to plot. Value=['blastn' | 'blastp']",
    )
    colours: list[str] = [
        "#fde725",
        "#90d743",
        "#35b779",
        "#21918c",
        "#31688e",
        "#443983",
        "#440154",
    ]  # viridis palette
    gradient: str = "#B22222"  # white to firebrick
    output_format: str = "SVG"
    graph_shape: str = "linear"
    graph_pagesize: str = "A4"
    graph_fragments: int = 1
    graph_start: int = 0
    graph_end: Optional[int] = None


@asset(
    description="Create a synteny diagram from the sequences to be plotted",
    required_resource_keys={"local_resource"},
    compute_kind="Biopython",
    metadata={
        "owner": OWNER,
    },
)
def create_graph(
    context, create_genome: dict, config: Diagram
) -> GenomeDiagram.Diagram:
    # Define the paths
    _gb_folder = context.resources.local_resource.get_paths()["GENBANK_DIR"]
    _synteny_folder = context.resources.local_resource.get_paths()["SYNTENY_DIR"]
    # Which blast summary table ?
    _tables_path = context.resources.local_resource.get_paths()["TABLES_DIR"]
    if config.graph_type == "blastp":
        _uniq_dir = str(Path(_tables_path) / "protein_uniqueness.parquet")
    else:
        _uniq_dir = str(Path(_tables_path) / "gene_uniqueness.parquet")

    _colour_dir = str(Path(_synteny_folder) / "colour_table.parquet")

    # Set name for the diagram
    _name_graph = config.title

    # Set colours for the graph
    _user_colours = config.colours
    if len(_user_colours) == 7:
        context.log.info("Colour palette all set!")
        colour_palette = _user_colours
    elif len(_user_colours) > 7:
        context.log.info(
            "Too many colours were passed! Only the first seven will be used!"
        )
        colour_palette = _user_colours[0:7]
    else:
        _missing_colours = 7 - len(_user_colours)
        context.log.info(
            f"Colour palette is missing {_missing_colours} colours! The graph will be plotted with the default palette"
        )
        colour_palette = [
            "#fde725",
            "#90d743",
            "#35b779",
            "#21918c",
            "#31688e",
            "#443983",
            "#440154",
        ]

    # _user_gradient = config.gradient
    # # if len(_user_gradient) == 2:
    # #     context.log.info("Colour gradient all set!")
    # #     colour_gradient = _user_gradient
    # # elif len(_user_gradient) > 2:
    # #     context.log.info(
    # #         "Too many colours were passed! Only the first two will be used for creating the gradient!"
    # #     )
    # #     colour_gradient = _user_gradient[0:2]
    # # else:
    # #     _missing_colours = 2 - len(_user_gradient)
    # #     context.log.info(
    # #         f"{_missing_colours} colour(s) missing to generate the gradient! The graph will be plotted with the default gradient"
    # #     )
    #     colour_gradient = ["#FFFFFF", "#B22222"]
    colour_gradient = config.gradient
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
            pl.read_parquet(_uniq_dir)
            .filter((pl.col("source_name") == _X) & (pl.col("query_name") == _Y))
            .select("source_locus_tag", "query_locus_tag", "percentage_of_identity")
        )

        for _id_X, _id_Y, _perc in _X_vs_Y.iter_rows():
            _color = colors.linearlyInterpolatedColor(
                colors.HexColor("#FFFFFF"),
                colors.HexColor(colour_gradient),
                0,
                100,
                _perc,
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
        # _gene_value = _assess_file_content(_record)
        _gene_value = [
            gbt["query_gb_type"]
            for gbt in pl.read_parquet(_uniq_dir)
            .group_by("query_name", "query_gb_type")
            .len()
            .iter_rows(named=True)
            if gbt["query_name"] == _record_name
        ][0]
        if _gene_value == "locus_tag" or _gene_value == "gene":
            for _feature in _record.features:
                if _feature.type != "gene":
                    # Exclude this feature
                    continue
                try:
                    if _gene_value == "locus_tag":
                        _perc = (
                            _gene_color_palette.filter(
                                (pl.col("query_name") == _record_name)
                                & (
                                    pl.col("query_locus_tag")
                                    == _feature.qualifiers["locus_tag"][0]
                                )
                            )
                            .select("perc_presence")
                            .item()
                        )
                        if _perc == 0:
                            _gene_color = colors.HexColor(colour_palette[0])
                        elif 0 < _perc <= 20:
                            _gene_color = colors.HexColor(colour_palette[1])
                        elif 20 < _perc <= 40:
                            _gene_color = colors.HexColor(colour_palette[2])
                        elif 40 < _perc <= 60:
                            _gene_color = colors.HexColor(colour_palette[3])
                        elif 60 < _perc <= 80:
                            _gene_color = colors.HexColor(
                                colour_palette[4]
                            )  # gene_color = colors.HexColor('#35b779')
                        elif 80 < _perc < 100:
                            _gene_color = colors.HexColor(
                                colour_palette[5]
                            )  # gene_color = colors.HexColor('#90d743')
                        elif _perc == 100:
                            _gene_color = colors.HexColor(
                                colour_palette[6]
                            )  # gene_color = colors.HexColor('#fde725')
                        else:
                            _gene_color = colors.black
                    else:
                        _perc = (
                            _gene_color_palette.filter(
                                (pl.col("query_name") == _record_name)
                                & (
                                    pl.col("query_locus_tag")
                                    == _feature.qualifiers["gene"][0]
                                )
                            )
                            .select("perc_presence")
                            .item()
                        )
                        if _perc == 0:
                            _gene_color = colors.HexColor(colour_palette[0])
                        elif 0 < _perc <= 20:
                            _gene_color = colors.HexColor(colour_palette[1])
                        elif 20 < _perc <= 40:
                            _gene_color = colors.HexColor(colour_palette[2])
                        elif 40 < _perc <= 60:
                            _gene_color = colors.HexColor(colour_palette[3])
                        elif 60 < _perc <= 80:
                            _gene_color = colors.HexColor(
                                colour_palette[4]
                            )  # gene_color = colors.HexColor('#35b779')
                        elif 80 < _perc < 100:
                            _gene_color = colors.HexColor(
                                colour_palette[5]
                            )  # gene_color = colors.HexColor('#90d743')
                        elif _perc == 100:
                            _gene_color = colors.HexColor(
                                colour_palette[6]
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
                    if _gene_value == "cds_locus_tag":
                        _perc = (
                            _gene_color_palette.filter(
                                (pl.col("query_name") == _record_name)
                                & (
                                    pl.col("query_locus_tag")
                                    == _feature.qualifiers["locus_tag"][0]
                                )
                            )
                            .select("perc_presence")
                            .item()
                        )
                        if _perc == 0:
                            _gene_color = colors.HexColor(colour_palette[0])
                        elif 0 < _perc <= 20:
                            _gene_color = colors.HexColor(colour_palette[1])
                        elif 20 < _perc <= 40:
                            _gene_color = colors.HexColor(colour_palette[2])
                        elif 40 < _perc <= 60:
                            _gene_color = colors.HexColor(colour_palette[3])
                        elif 60 < _perc <= 80:
                            _gene_color = colors.HexColor(
                                colour_palette[4]
                            )  # gene_color = colors.HexColor('#35b779')
                        elif 80 < _perc < 100:
                            _gene_color = colors.HexColor(
                                colour_palette[5]
                            )  # gene_color = colors.HexColor('#90d743')
                        elif _perc == 100:
                            _gene_color = colors.HexColor(
                                colour_palette[6]
                            )  # gene_color = colors.HexColor('#fde725')
                        else:
                            _gene_color = colors.black
                    else:
                        _perc = (
                            _gene_color_palette.filter(
                                (pl.col("query_name") == _record_name)
                                & (
                                    pl.col("query_locus_tag")
                                    == _feature.qualifiers["protein_id"][0]
                                )
                            )
                            .select("perc_presence")
                            .item()
                        )
                        if _perc == 0:
                            _gene_color = colors.HexColor(colour_palette[0])
                        elif 0 < _perc <= 20:
                            _gene_color = colors.HexColor(colour_palette[1])
                        elif 20 < _perc <= 40:
                            _gene_color = colors.HexColor(colour_palette[2])
                        elif 40 < _perc <= 60:
                            _gene_color = colors.HexColor(colour_palette[3])
                        elif 60 < _perc <= 80:
                            _gene_color = colors.HexColor(
                                colour_palette[4]
                            )  # gene_color = colors.HexColor('#35b779')
                        elif 80 < _perc < 100:
                            _gene_color = colors.HexColor(
                                colour_palette[5]
                            )  # gene_color = colors.HexColor('#90d743')
                        elif _perc == 100:
                            _gene_color = colors.HexColor(
                                colour_palette[6]
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

    if config.graph_end:
        graph_end = config.graph_end
    else:
        graph_end = _max_len

    _gd_diagram.draw(
        format=config.graph_shape,
        pagesize=config.graph_pagesize,
        fragments=config.graph_fragments,
        start=config.graph_start,
        end=graph_end,
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

    # Fixed labels left
    text_elements = root.xpath('.//*[local-name()="text"]')

    # Filter predicates
    _has_key = (
        lambda x: x.text
        and (x.text.strip() != "")
        and (x.text.strip() in _record_names)
    )
    text_labels = list(filter(_has_key, text_elements))

    for t in text_labels:
        t.attrib["transform"] = "translate(-40,0) scale(1,-1)"
        label = t.text
        title = etree.SubElement(t, "title")
        title.text = t.text
        if len(label) > 8:
            label = label[:3] + ".." + label[-3:]
        t.text = label

    with open(_path_output, "wb") as label_writer:
        label_writer.write(etree.tostring(root))

    context.log.info(f"W: {width}, H: {height}")

    xpos = int(math.trunc(width * 0.68))
    ypos = int(math.trunc(height * 0.99))
    context.log.info(f"Coord of SVG: {str(xpos)} : {str(ypos)}")

    # Prepare legend:
    r, g, b = ImageColor.getcolor(colour_gradient, "RGB")
    GRADIENT = f"{r},{g},{b}"
    CB_1 = colour_palette[0]
    CB_2 = colour_palette[1]
    CB_3 = colour_palette[2]
    CB_4 = colour_palette[3]
    CB_5 = colour_palette[4]
    CB_6 = colour_palette[5]
    CB_7 = colour_palette[6]

    parent = Path(__file__).parent
    context.log.info(f"parent: {parent}")
    svg = Template(open(str(parent / "template.svg")).read())
    legend = svg.substitute(
        gradient=GRADIENT,
        cb_1=CB_1,
        cb_2=CB_2,
        cb_3=CB_3,
        cb_4=CB_4,
        cb_5=CB_5,
        cb_6=CB_6,
        cb_7=CB_7,
    )

    _uniq_legend_id = datetime.timestamp(
        datetime.now()
    )  # int(datetime.timestamp(datetime.now()))
    legend_path = str(Path(TEMP_DIR) / f"{_uniq_legend_id}_legend.svg")
    with open(legend_path, "w") as writer:
        writer.write(legend)

    # (f"{_synteny_folder}/legend.svg")
    C.Figure(
        f"{width}px",
        f"{height}px",
        C.SVG(_path_output).scale(0.96),
        C.SVG(legend_path).move(xpos, ypos).scale(0.9),
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
            "text_metadata": "A new synteny diagram has been created.",
            "graph_type": config.graph_type,
            "num_sqcs": len(_records),
            "folder": _path_output,
            "sequences": MetadataValue.json(_record_names),
            "synteny_overview": MetadataValue.md(
                f"![img](data:image/png;base64,{image_data.decode()})"
            ),
        }
    )

    return _gd_diagram
