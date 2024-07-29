from dagster import (
    Config,
    op,
    DynamicOut,
    DynamicOutput,
    graph_asset,
    In,
    Nothing,
    OpExecutionContext,
    AssetKey,
    Output,
    MetadataValue,
)

import os
import duckdb
import polars as pl

from pathlib import Path
from functools import partial
from synphage.resources.local_resource import OWNER


class NPipeConfig(Config):
    # source: str
    sql: str = "sql/parse_blastn.sql"
    target: str = "blastn_parsing"
    # table_dir: str = None
    file: str = "blastn_summary.parquet"
    locus_table: str = "processed_genbank_df.parquet"


@op
def setup_nconfig(config: NPipeConfig) -> NPipeConfig:
    """Source/target dirs and file output"""
    return config


@op(
    out=DynamicOut(),
    required_resource_keys={"local_resource"},
)
def nload(context: OpExecutionContext, get_blastn):
    """Load blastn output files"""
    for file in get_blastn.new:
        yield DynamicOutput(file, mapping_key=file.replace(".", "_"))


@op(
    required_resource_keys={"local_resource"},
)
def parse_blastn(
    context: OpExecutionContext, setup_nconfig: NPipeConfig, file: str
):  # setup_nconfig: NPipeConfig, file: str):
    """Retrieve blastn results and store it in a DataFrame"""
    _path_parse_blastn_sql = os.path.join(
        os.path.dirname(__file__), "sql/parse_blastn.sql"
    )
    context.log.info(f"sql_file: {_path_parse_blastn_sql}")
    query = open(_path_parse_blastn_sql).read()
    conn = duckdb.connect(":memory:")

    fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
    bn = context.resources.local_resource.get_paths()["BLASTN_DIR"]
    # context.log.info(f"{setup_nconfig.target}/{file}.parquet")
    # context.log.info(f"File: {file}")
    source = str(Path(bn) / file)
    destination = str(Path(fs) / setup_nconfig.target)
    os.makedirs(destination, exist_ok=True)
    context.log.info(f"File: {file}")
    # context.log.info(f"source: {source}")
    # context.log.info(f"destination: {destination}.parquet")
    conn.query(query.format(source)).pl().write_parquet(
        str(Path(destination) / f"{file}.parquet")
    )
    return "OK"


@op(
    ins={"file": In(Nothing)},
    required_resource_keys={"local_resource"},
)
def append_blastn(context: OpExecutionContext, setup_nconfig: NPipeConfig):
    """Consolidate in 1 parquet file"""
    path = context.resources.local_resource.get_paths()["TABLES_DIR"]
    os.makedirs(path, exist_ok=True)
    path_file = str(Path(path) / setup_nconfig.file)
    fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
    source = str(Path(fs) / setup_nconfig.target)
    pl.read_parquet(f"{source}/*.parquet").write_parquet(path_file)
    return path_file


@op(
    ins={
        "blastn_all": In(asset_key=AssetKey("append_blastn")),
        #         "locus_all": In(asset_key=AssetKey("append_locus")),
    },
    required_resource_keys={"local_resource"},
)
def gene_presence(
    context: OpExecutionContext, setup_nconfig: NPipeConfig, blastn_all: str
):  # , locus_all):
    """Consolidate gene and locus"""
    tables = context.resources.local_resource.get_paths()["TABLES_DIR"]
    path_to_df = str(Path(tables) / setup_nconfig.locus_table)
    # locus_all = pl.read_parquet(path_to_df)

    conn = duckdb.connect(":memory:")
    _path_gene_presence_sql = os.path.join(
        os.path.dirname(__file__), "sql/gene_presence.sql"
    )
    context.log.info(f"sql_file: {_path_gene_presence_sql}")
    query = open(_path_gene_presence_sql).read()
    conn.query(query.format(blastn_all, path_to_df)).pl().write_parquet(
        str(Path(tables) / "gene_uniqueness.parquet")
    )

    df = pl.read_parquet(str(Path(tables) / "gene_uniqueness.parquet"))

    return Output(
        value="ok",
        metadata={
            "table_location": str(Path(tables) / "gene_uniqueness.parquet"),
            "num_rows": len(df),
            "preview": MetadataValue.md(df.to_pandas().head().to_markdown()),
        },
    )


@graph_asset(
    description="Create a new DataFrame, joining information from the blastn results with the dataset information",
    metadata={"owner": OWNER},
)
def transform_blastn(get_blastn):
    config_blastn = setup_nconfig.alias("blastn")()
    files = nload.alias("nload_blastn")(get_blastn)
    results = files.map(partial(parse_blastn, config_blastn))
    blastn_all = append_blastn.alias("append_blastn")(config_blastn, results.collect())

    return gene_presence(config_blastn, blastn_all=blastn_all)
