from dagster import (
    Config,
    op,
    DynamicOut,
    DynamicOutput,
    graph_asset,
    asset,
    In,
    Nothing,
    AssetSpec,
)

import os
import pickle
import duckdb
import polars as pl
import pandas as pd

from pathlib import Path
from collections import namedtuple
from functools import partial

from synphage.utils.convert_gb_to_df import genbank_to_dataframe


GenbankRecord = namedtuple("GenbankRecord", "new,history")


@asset(
    deps=[
        AssetSpec("download_to_genbank", skippable=True),
        AssetSpec("users_to_genbank", skippable=True),
    ],
    required_resource_keys={"local_resource"},
    description="Keep track of the genbank files that have been processed",
    compute_kind="Python",
    io_manager_key="io_manager",
    metadata={"owner": "Virginie Grosboillot"},
)
def genbank_history(context) -> GenbankRecord:
    # load genbank history
    fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
    _hist_gb_path = str(Path(fs) / "genbank_history")
    if os.path.exists(_hist_gb_path):
        _history_files = pickle.load(open(_hist_gb_path, "rb")).history
        context.log.info("History genbank files loaded")
    else:
        _history_files = []
        context.log.info("No genbank history available")

    # Path to genbank folder
    _gb_path = context.resources.local_resource.get_paths()["GENBANK_DIR"]
    os.makedirs(_gb_path, exist_ok=True)

    # Path to genbank folder
    _gb_path = context.resources.local_resource.get_paths()["GENBANK_DIR"]
    os.makedirs(_gb_path, exist_ok=True)

    # Unprocessed files
    _new_files = list(set(os.listdir(_gb_path)).difference(set(_history_files)))
    context.log.info(f"Number of genbank files to be processed: {len(_new_files)}")

    _new_items = []
    for _file in _new_files:
        _new_items.append(str(Path(_file).stem))

    _updated_history_files = [*_history_files, *_new_items]

    context.add_output_metadata(
        metadata={
            "path": _gb_path,
            "num_new_files": len(_new_files),
            "new_files_preview": _new_files,
            "total_files": len(_updated_history_files),
            "total_files_preview": _updated_history_files,
        },
    )

    return GenbankRecord(_new_items, _updated_history_files)


class ValidationConfig(Config):  # type: ignore[misc] # should be ok in 1.8 version of Dagster
    target_suffix: str = "gb_parsing"
    table_dir_suffix: str = "genbank_db"


@op
def setup_validation_config(config: ValidationConfig) -> ValidationConfig:
    """Configuration for genbank table"""
    return config


@op(
    out=DynamicOut(),
    required_resource_keys={"local_resource"},
)
def load_gb(context, genbank_history):
    """Load GenBank files"""
    for file in genbank_history.new:
        yield DynamicOutput(file, mapping_key=file.replace(".", "_"))


@op(
    required_resource_keys={"local_resource"},
)
def parse_gb(context, setup_config: ValidationConfig, file: str):
    """Retrieve information from genbank files and store them in dataframes"""
    # Storage path to individual dataframes
    target = str(
        Path(context.resources.local_resource.get_paths()["FILESYSTEM_DIR"])
        / setup_config.target_suffix
    )
    os.makedirs(target, exist_ok=True)
    # Process file
    source = context.resources.local_resource.get_paths()["GENBANK_DIR"]
    full_path = str(Path(source) / f"{file}.gb")
    df = genbank_to_dataframe(full_path)
    df.write_parquet(f"{target}/{file}.parquet")
    return df


@op(
    ins={"file": In(Nothing)},
    required_resource_keys={"local_resource"},
)
def append_gb(context, setup_config: ValidationConfig):
    """Collect all the dataframes in one unique dataframe"""
    target = str(
        Path(context.resources.local_resource.get_paths()["FILESYSTEM_DIR"])
        / setup_config.target_suffix
    )
    path_file = context.resources.local_resource.get_paths()["TABLES_DIR"]
    os.makedirs(path_file, exist_ok=True)
    parquet_origin = f"{target}/*.parquet"
    parquet_destination = str(Path(path_file) / "setup_config.table_dir_suffix")

    
    (
        duckdb
        .connect(":memory:") 
        .execute("""
                CREATE or REPLACE TABLE genbank (
                cds_gene string, cds_locus_tag string, protein_id string, function string, product string, translation string, transl_table string, codon_start string,
                start_sequence integer, end_sequence integer, strand integer, extract string, gene string, locus_tag string, translation_fn string, id string, name string, description string, topology string, organism string, 
                taxonomy varchar[], filename string);"""
        )
        .execute(f"INSERT INTO genbank by position (select * from read_parquet('{parquet_origin}'))")
        .execute("select * from genbank")
        .pl()
        .write_parquet(parquet_destination)
    )
    return "ok"


@graph_asset(
    description="Create a genbank DataFrame",
    metadata={"owner": "Virginie Grosboillot"},
)
def create_genbank_df(genbank_history):  # download_to_genbank, users_to_genbank
    config_gb = setup_validation_config()
    files = load_gb(genbank_history)
    results = files.map(partial(parse_gb, config_gb))
    all_gb = append_gb(config_gb, results.collect())
    return all_gb
