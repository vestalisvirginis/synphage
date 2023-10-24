from dagster import (
    AssetSelection,
    define_asset_job,
    ScheduleDefinition,
    op,
    job,
    DynamicOutput,
    DynamicOut,
    Nothing,
    In,
)

import os
import duckdb
import polars as pl


# from .sensors import genbank_file_update_sensor

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


# Job parsing blastn files

import os
from dagster import op, job, DynamicOutput, DynamicOut, Definitions, Nothing, In, Config, AssetKey, ExperimentalWarning
import polars as pl
import duckdb
from functools import partial, reduce
from Bio import SeqIO
from operator import methodcaller as mc
from operator import attrgetter as at
from operator import itemgetter as it
from operator import eq
from toolz import first, compose
from pathlib import Path
import warnings
warnings.filterwarnings("ignore", category=ExperimentalWarning)


class PipeConfig(Config):
    source: str
    target: str = None
    file: str = "out.parquet"


@op
def setup(config: PipeConfig) -> PipeConfig:
    """Source/target dirs and    file output"""
    return config


@op(out=DynamicOut())
def load(setup: PipeConfig):
    """Load GenBank files"""
    for file in os.listdir(setup.source):
        yield DynamicOutput(file, mapping_key=file.replace(".", "_"))


@op
def parse_blastn(setup: PipeConfig, file: str):
    """Retrive sequence and metadata"""
    query = open("sql/parse_blastn.sql").read()
    conn = duckdb.connect(":memory:")
    os.makedirs(setup.target, exist_ok=True)
    conn.query(query.format(file)).pl().write_parquet(f"{setup.target}/{file}.parquet")
    return "OK"


@op
def parse_locus(setup: PipeConfig, file: str):
    """Retrieve gene and locus metadata"""
    source = Path(setup.source) / file
    target = Path(setup.target) / str(file+".parquet")
    genome = SeqIO.read(str(source), "gb")

    # Ancilliary Functions
    _type = at("type")
    _type_gene = compose(partial(eq, "gene"), _type)
    _type_cds = compose(partial(eq, "CDS"), _type)
    
    _locus = compose(first,  mc("get", "locus_tag", [""]), at("qualifiers"))
    _gene = compose(first,  mc("get", "gene", [""]), at("qualifiers"))
    _protein = compose(it(slice(-2)), first, mc("get", "protein_id", ""), at("qualifiers"))
    _fn_gene = lambda x: (genome.name, _gene(x), _locus(x))
    _fn_cds = lambda x: (genome.name, _protein(x), _protein(x))

    # DataFrame structure
    schema = ["name", "gene", "locus_tag"]

    if set(map(_type, filter(_type_gene, genome.features))):
        data = list(map(_fn_gene, filter(_type_gene, genome.features)))
    elif set(map(_type, filter(_type_cds, genome.features))):
        data = list(map(_fn_cds, filter(_type_cds, genome.features)))

    os.makedirs(setup.target, exist_ok=True)
    pl.DataFrame(data=data, schema=schema, orient="row").write_parquet(str(target))
    return "OK"


@op(ins={"file": In(Nothing)})
def append(setup: PipeConfig):
    """Consolidate in 1 parquet file"""
    pl.read_parquet(f"{setup.target}/*.parquet").write_parquet(setup.file)
    return setup.file


@op(ins={"blastn_all": In(asset_key=AssetKey("append_blastn")), "locus_all": In(asset_key=AssetKey("append_locus"))})
def gene_presence(blastn_all, locus_all):
    """Consolidate gene and locus"""
    conn = duckdb.connect(":memory:")
    query = open("sql/gene_presence.sql").read()
    conn.query(query.format(blastn_all, locus_all)).pl().write_parquet("presence.parquet")


@job
def transform():
    """GenBank into parquet"""
    config_blastn = setup.alias("blastn")()
    files = load.alias("load_blastn")(config_blastn)
    results = files.map(partial(parse_blastn, config_blastn))
    blastn_all = append.alias("append_blastn")(config_blastn, results.collect())

    config_locus = setup.alias("locus")()
    files_locus = load.alias("load_locus")(config_locus)
    results_locus = files_locus.map(partial(parse_locus, config_locus))
    locus_all = append.alias("append_locus")(config_locus, results_locus.collect())

    gene_presence(blastn_all=blastn_all, locus_all=locus_all)


#defs = Definitions(jobs=[transform])



# asset_job_sensor = genbank_file_update_sensor(
#     define_asset_job(
#         name="load_job",
#         selection=AssetSelection.groups("Status")
#         | (
#             AssetSelection.groups("Blaster")
#             & AssetSelection.keys("process_asset").downstream()
#         )
#         | AssetSelection.keys("extract_locus_tag_gene"),
#         tags={"dagster/priority": "0"},
#     )
# )


# Job triggering the json files parsing
parsing_job = define_asset_job(
    name="parsing_job",
    selection=AssetSelection.keys("parse_blastn").required_multi_asset_neighbors(),
)

parsing_schedule = ScheduleDefinition(
    job=parsing_job,
    cron_schedule="*/1 * * * *",  # every minute
    tags={"dagster/priority": "1"},
)


# Job triggering the update of the last tables based on Locus_and_gene and blastn dataframes
uniq_job = define_asset_job(
    name="uniq_job",
    selection=AssetSelection.keys("gene_presence_table"),
)

uniq_schedule = ScheduleDefinition(
    job=uniq_job,
    cron_schedule="*/5 * * * *",  # every hour minute???
    tags={"dagster/priority": "2"},
)
