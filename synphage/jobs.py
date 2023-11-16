from dagster import (
    AssetSelection,
    define_asset_job,
    op,
    job,
    DynamicOutput,
    DynamicOut,
    Nothing,
    In,
    RunConfig,
    Config,
    AssetKey,
    ExperimentalWarning,
)

import os
import duckdb
import polars as pl
from functools import partial
from Bio import SeqIO
from operator import methodcaller as mc
from operator import attrgetter as at
from operator import itemgetter as it
from operator import eq
from toolz import first, compose
from pathlib import Path
import warnings

warnings.filterwarnings("ignore", category=ExperimentalWarning)


# Job 1 -> get the list of genbank to blastn
blasting_job = define_asset_job(
    name="blasting_job",
    selection=AssetSelection.groups("Status")
    | (
        AssetSelection.groups("Blaster")
        & AssetSelection.keys("new_fasta_files")
        .required_multi_asset_neighbors()
        .downstream()
    ),
)


# Job 2 parsing blastn files and locus -> create uniqueness DataFrame


class PipeConfig(Config):
    source: str
    target: str = None
    table_dir: str = None
    file: str = "out.parquet"


@op
def setup_config(config: PipeConfig) -> PipeConfig:
    """Source/target dirs and    file output"""
    return config


@op(out=DynamicOut())
def load(context, setup_config: PipeConfig):
    """Load GenBank files"""
    context.log.info(setup_config.source)
    for file in os.listdir(setup_config.source):
        yield DynamicOutput(file, mapping_key=file.replace(".", "_"))


@op
def parse_blastn(context, setup_config: PipeConfig, file: str):
    """Retrive sequence and metadata"""
    _path_parse_blastn_sql = os.path.join(
        os.path.dirname(__file__), "sql/parse_blastn.sql"
    )
    context.log.info(f"sql_file: {_path_parse_blastn_sql}")
    query = open(_path_parse_blastn_sql).read()
    conn = duckdb.connect(":memory:")
    os.makedirs(setup_config.target, exist_ok=True)
    context.log.info(f"{setup_config.target}/{file}.parquet")
    context.log.info(f"File: {file}")
    source = Path(setup_config.source) / file
    context.log.info(f"source: {source}")
    conn.query(query.format(source)).pl().write_parquet(
        f"{setup_config.target}/{file}.parquet"
    )
    return "OK"


@op
def parse_locus(setup_config: PipeConfig, file: str):
    """Retrieve gene and locus metadata"""
    source = Path(setup_config.source) / file
    target = Path(setup_config.target) / str(Path(file).stem + ".parquet")
    genome = SeqIO.read(str(source), "gb")

    # Ancilliary Functions
    _type = at("type")
    _type_gene = compose(partial(eq, "gene"), _type)
    _type_cds = compose(partial(eq, "CDS"), _type)

    _locus = compose(first, mc("get", "locus_tag", [""]), at("qualifiers"))
    _gene = compose(first, mc("get", "gene", [""]), at("qualifiers"))
    _protein = compose(
        it(slice(-2)), first, mc("get", "protein_id", ""), at("qualifiers")
    )
    _fn_gene = lambda x: (genome.name, _gene(x), _locus(x))
    _fn_cds = lambda x: (genome.name, _protein(x), _protein(x))

    # DataFrame structure
    schema = ["name", "gene", "locus_tag"]

    if set(map(_type, filter(_type_gene, genome.features))):
        data = list(map(_fn_gene, filter(_type_gene, genome.features)))
    elif set(map(_type, filter(_type_cds, genome.features))):
        data = list(map(_fn_cds, filter(_type_cds, genome.features)))

    os.makedirs(setup_config.target, exist_ok=True)
    pl.DataFrame(data=data, schema=schema, orient="row").write_parquet(str(target))
    return "OK"


@op(ins={"file": In(Nothing)})
def append(setup_config: PipeConfig):
    """Consolidate in 1 parquet file"""
    os.makedirs(setup_config.table_dir, exist_ok=True)
    path_file = Path(setup_config.table_dir) / Path(setup_config.file)
    pl.read_parquet(f"{setup_config.target}/*.parquet").write_parquet(path_file)
    return path_file


@op(
    ins={
        "blastn_all": In(asset_key=AssetKey("append_blastn")),
        "locus_all": In(asset_key=AssetKey("append_locus")),
    }
)
def gene_presence(context, blastn_all, locus_all):
    """Consolidate gene and locus"""
    conn = duckdb.connect(":memory:")
    _path_gene_presence_sql = os.path.join(
        os.path.dirname(__file__), "sql/gene_presence.sql"
    )
    context.log.info(f"sql_file: {_path_gene_presence_sql}")
    query = open(_path_gene_presence_sql).read()
    conn.query(query.format(blastn_all, locus_all)).pl().write_parquet(
        "/data/tables/uniqueness.parquet"
    )


default_config = RunConfig(
    ops={
        "blastn": PipeConfig(
            source="/".join([os.getenv("PHAGY_DIRECTORY"), "gene_identity/blastn"]),
            target="/".join(
                [
                    os.getenv("PHAGY_DIRECTORY"),
                    os.getenv("FILE_SYSTEM"),
                    "blastn_parsing",
                ]
            ),
            table_dir="/".join([os.getenv("PHAGY_DIRECTORY"), "tables"]),
            file="blastn_summary.parquet",
        ),
        "locus": PipeConfig(
            source="/".join([os.getenv("PHAGY_DIRECTORY"), "genbank"]),
            target="/".join(
                [
                    os.getenv("PHAGY_DIRECTORY"),
                    os.getenv("FILE_SYSTEM"),
                    "locus_parsing",
                ]
            ),
            table_dir="/".join([os.getenv("PHAGY_DIRECTORY"), "tables"]),
            file="locus_and_gene.parquet",
        ),
    }
)


@job(config=default_config)
def transform():
    """GenBank into parquet"""
    config_blastn = setup_config.alias("blastn")()
    files = load.alias("load_blastn")(config_blastn)
    results = files.map(partial(parse_blastn, config_blastn))
    blastn_all = append.alias("append_blastn")(config_blastn, results.collect())

    config_locus = setup_config.alias("locus")()
    files_locus = load.alias("load_locus")(config_locus)
    results_locus = files_locus.map(partial(parse_locus, config_locus))
    locus_all = append.alias("append_locus")(config_locus, results_locus.collect())

    gene_presence(blastn_all=blastn_all, locus_all=locus_all)


# Job 3 -> creates the synteny diagram

synteny_job = define_asset_job(
    name="synteny_job",
    selection=AssetSelection.groups("Viewer"),
)
