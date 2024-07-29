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


class PPipeConfig(Config):
    # source: str
    sql: str = "sql/parse_blastn.sql"
    target: str = "blastp_parsing"
    # table_dir: str = None
    file: str = "blastp_summary.parquet"
    locus_table: str = "processed_genbank_df.parquet"


@op
def setup_pconfig(config: PPipeConfig) -> PPipeConfig:
    """Source/target dirs and file output"""
    return config


@op(
    out=DynamicOut(),
    required_resource_keys={"local_resource"},
)
def pload(context: OpExecutionContext, get_blastp):
    """Load blastp output files"""
    for file in get_blastp.new:
        yield DynamicOutput(file, mapping_key=file.replace(".", "_"))


@op(
    required_resource_keys={"local_resource"},
)
def parse_blastp(
    context: OpExecutionContext, setup_pconfig: PPipeConfig, file: str
):  # setup_pconfig: PPipeConfig, file: str):
    """Retrieve blastp results and store it in a DataFrame"""
    _path_parse_blastp_sql = os.path.join(
        os.path.dirname(__file__), "sql/parse_blastn.sql"
    )
    context.log.info(f"sql_file: {_path_parse_blastp_sql}")
    query = open(_path_parse_blastp_sql).read()
    conn = duckdb.connect(":memory:")

    fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
    bn = context.resources.local_resource.get_paths()["BLASTP_DIR"]
    # context.log.info(f"{setup_pconfig.target}/{file}.parquet")
    # context.log.info(f"File: {file}")
    source = str(Path(bn) / file)
    destination = str(Path(fs) / setup_pconfig.target)
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
def append_blastp(context: OpExecutionContext, setup_pconfig: PPipeConfig):
    """Consolidate in 1 parquet file"""
    path = context.resources.local_resource.get_paths()["TABLES_DIR"]
    os.makedirs(path, exist_ok=True)
    path_file = str(Path(path) / setup_pconfig.file)
    fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
    source = str(Path(fs) / setup_pconfig.target)
    pl.read_parquet(f"{source}/*.parquet").write_parquet(path_file)
    return path_file


@op(
    ins={
        "blastp_all": In(asset_key=AssetKey("append_blastp")),
        #         "locus_all": In(asset_key=AssetKey("append_locus")),
    },
    required_resource_keys={"local_resource"},
)
def protein_presence(
    context: OpExecutionContext, setup_pconfig: PPipeConfig, blastp_all
):  # , locus_all):
    """Consolidate gene and locus"""
    tables = context.resources.local_resource.get_paths()["TABLES_DIR"]
    path_to_df = str(Path(tables) / setup_pconfig.locus_table)
    # locus_all = pl.read_parquet(path_to_df)

    conn = duckdb.connect(":memory:")
    _path_protein_presence_sql = os.path.join(
        os.path.dirname(__file__), "sql/gene_presence.sql"
    )
    context.log.info(f"sql_file: {_path_protein_presence_sql}")
    query = open(_path_protein_presence_sql).read()
    conn.query(query.format(blastp_all, path_to_df)).pl().write_parquet(
        str(Path(tables) / "protein_uniqueness.parquet")
    )

    df = pl.read_parquet(str(Path(tables) / "protein_uniqueness.parquet"))

    return Output(
        value="ok",
        metadata={
            "table_location": str(Path(tables) / "protein_uniqueness.parquet"),
            "num_rows": len(df),
            "preview": MetadataValue.md(df.to_pandas().head().to_markdown()),
        },
    )


@graph_asset(
    description="Create a new DataFrame, joining information from the blastp results with the dataset information",
    metadata={"owner": OWNER},
)
def transform_blastp(get_blastp):
    config_blastp = setup_pconfig.alias("blastp")()
    files = pload.alias("load_blastp")(get_blastp)
    results = files.map(partial(parse_blastp, config_blastp))
    blastp_all = append_blastp.alias("append_blastp")(config_blastp, results.collect())

    return protein_presence(config_blastp, blastp_all=blastp_all)
