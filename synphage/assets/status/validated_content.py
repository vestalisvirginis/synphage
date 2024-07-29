from dagster import (
    asset,
    AssetSpec,
    Output,
    AssetCheckResult,
    MetadataValue,
)

import os
import tempfile
import duckdb
import polars as pl
import csv

from pathlib import Path


from synphage.utils.check_factory import (
    _check_severity,
    _create_check_specs,
)
from synphage.resources.local_resource import OWNER


TEMP_DIR = tempfile.gettempdir()

PATH_TO_LOCAL_DIR = str(Path(os.getenv("OUTPUT_DIR", TEMP_DIR)) / "fs")
PATH_TO_DVP_DIR = str(Path("temp") / "development" / "data" / "fs")


def _get_upstream_asset_names() -> list[AssetSpec]:
    _path = PATH_TO_LOCAL_DIR
    if os.path.exists(_path):
        return [
            AssetSpec(file, skippable=True)
            for file in os.listdir(_path)
            if file.endswith("_transformation")
        ]


PROCESSED_GB_CHECKS = {
    "is_complete": {
        "check_type": "std",
        "check_name": "is_complete",
        "cols": ["locus_tag", "key"],
        "check_value": None,
        "check_description": "Validate the column completeness",
    },
    "is_unique": {
        "check_type": "std",
        "check_name": "is_unique",
        "cols": ["locus_tag", "key"],
        "check_value": None,
        "check_description": "Validate the uniqueness of each value presents in the column",
    },
}

check, check_specs = _create_check_specs(
    PROCESSED_GB_CHECKS, asset_name="append_processed_df"
)


@asset(
    deps=_get_upstream_asset_names(),
    required_resource_keys={"local_resource"},
    description="Keep track of the genbank files that have been processed",
    check_specs=check_specs,
    compute_kind="Python",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def append_processed_df(context):
    """Collect all the validated dataframes in one unique dataframe"""
    target = str(
        Path(context.resources.local_resource.get_paths()["FILESYSTEM_DIR"])
        / "transformed_dfs"
    )
    path_file = context.resources.local_resource.get_paths()["TABLES_DIR"]
    os.makedirs(path_file, exist_ok=True)
    # if os.path.exists(target):
    parquet_origin = f"{target}/*.parquet"
    parquet_destination = str(Path(path_file) / "processed_genbank_df.parquet")
    df = (
        duckdb.connect(":memory:")
        .execute(
            """
                CREATE or REPLACE TABLE genbank (
                cds_gene string, cds_locus_tag string, protein_id string, function string, product string, translation string, transl_table string, codon_start string,
                start_sequence integer, end_sequence integer, strand integer, cds_extract string, gene string, locus_tag string, extract string, translation_fn string, id string, name string, description string, topology string, organism string, 
                taxonomy varchar[], filename string, gb_type string);"""
        )
        .execute(
            f"INSERT INTO genbank by position (select * from read_parquet('{parquet_origin}'))"
        )
        .execute("select * from genbank")
        .pl()
        .with_columns(pl.concat_str("filename", "id", "locus_tag").hash().alias("key"))
    )
    df.write_parquet(parquet_destination)
    # df = pl.read_parquet(f"{target}/*.parquet").with_columns(
    #     pl.concat_str("filename", "id", "locus_tag").hash().alias("key")
    # )
    # df.write_parquet(str(Path(path_file) / "processed_genbank_df.parquet"))

    check_df = check.validate(df)

    for item in check_df.iter_rows(named=True):
        yield AssetCheckResult(
            asset_key=str("append_processed_df").lower(),
            check_name=f"{item['rule']}.{item['column']}",
            passed=(item["status"] == "PASS"),
            metadata={
                "level": item["level"],
                "rows": int(item["rows"]),
                "column": item["column"],
                "value": str(item["value"]),
                "violations": int(item["violations"]),
                "pass_rate": item["pass_rate"],
            },
            severity=_check_severity(check),
        )

    csv_path = str(
        Path(context.resources.local_resource.get_paths()["SYNPHAGE_DATA"])
        / "sequences.csv"
    )
    seq_dict = {}
    for file in [
        Path(filename["filename"]).name
        for filename in df.select("filename").unique().iter_rows(named=True)
    ]:
        seq_dict[file] = 0

    with open(csv_path, "w") as f:
        w = csv.writer(f)
        w.writerows(seq_dict.items())

    yield Output(
        value=(df, seq_dict, check_df),
        metadata={
            "rows_data": len(df),
            "df": MetadataValue.md(df.to_pandas().head().to_markdown()),
            "num_sequences": len(seq_dict),
            "rows_check_df": len(check_df),
            "check_df": MetadataValue.md(check_df.to_pandas().to_markdown()),
        },
    )


# for id , name check on the full dataframe --> unique to each file
# are_unique
# cols = [(file, id), (file, name)]
