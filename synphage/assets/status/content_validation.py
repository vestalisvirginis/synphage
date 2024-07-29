from dagster import (
    Output,
    AssetCheckSpec,
    AssetCheckResult,
    asset,
    AssetIn,
    AssetExecutionContext,
    MetadataValue,
    file_relative_path,
    ExperimentalWarning,
)

import os
import pickle
import shutil
import duckdb
import polars as pl
import tempfile
import warnings
from pathlib import Path
from Bio.Seq import translate

from synphage.utils.check_factory import _check_severity, _create_check_specs
from synphage.resources.local_resource import OWNER

warnings.filterwarnings("ignore", category=ExperimentalWarning)


TEMP_DIR = tempfile.gettempdir()

PATH_TO_LOCAL_DIR = str(
    Path(os.getenv("OUTPUT_DIR", TEMP_DIR)) / "fs" / "genbank_history"
)
PATH_TO_DVP_DIR = str(Path("temp") / "development" / "data" / "fs" / "genbank_history")

GENBANK_CHECKS = {
    "is_complete": {
        "check_type": "std",
        "check_name": "is_complete",
        "cols": [
            "gene",
            "locus_tag",
            "cds_gene",
            "cds_locus_tag",
            "protein_id",
            "start_sequence",
            "end_sequence",
            "strand",
            "extract",
        ],
        "check_value": None,
        "check_description": "Validate the column completeness",
    },
    "is_unique": {
        "check_type": "std",
        "check_name": "is_unique",
        "cols": [
            "gene",
            "locus_tag",
            "cds_gene",
            "cds_locus_tag",
            "protein_id",
            "start_sequence",
            "end_sequence",
        ],
        "check_value": None,
        "check_description": "Validate the uniqueness of each value presents in the column",
    },
    "is_dna": {
        "check_type": "bio",
        "check_name": "is_dna",
        "cols": ["extract"],
        "check_value": None,
        "check_description": "Validate that the sequence only contains ATCG",
    },
    "is_protein": {
        "check_type": "bio",
        "check_name": "is_protein",
        "cols": ["translation", "translation_fn"],
        "check_value": None,
        "check_description": "Validate that the protein sequence only contains valid amino acids",
    },
    "is_contained_in": {
        "check_type": "parameterised",
        "check_name": "is_contained_in",
        "cols": ["strand"],
        "check_value": (1, -1),
        "check_description": "Validate that the protein sequence only contains valid amino acids",
    },
}

# Check logic to implement:
#   if cds_locus_tag, check locus_tag == cds_locus_tag
#   bio checks:
#       levenstein distance
#       cols=[('translate', 'translate_fn')]

#   for id , name check on the full dataframe --> unique to each file
#       are_unique
#       cols = [(file, id), (file, name)]


def _gb_validation_settings(
    asset_name: str,
    asset_check_specs: list[AssetCheckSpec],
    asset_description: str = "Empty",
) -> dict:
    """Asset configuration for genbank file validation assets"""
    return {
        "name": asset_name.lower(),
        "description": asset_description,
        "io_manager_key": "io_manager",
        "required_resource_keys": {"local_resource"},
        "metadata": {"owner": OWNER},
        "compute_kind": "Validation",
        "check_specs": asset_check_specs,
    }


def _gb_labelling_settings(
    asset_name: str, input_name: str, asset_description: str = "Empty"
) -> dict:
    """Asset configuration for check evaluation and genbank file labelling"""
    return {
        "name": asset_name.lower(),
        "description": asset_description,
        "ins": {input_name.lower(): AssetIn()},
        "io_manager_key": "io_manager",
        "metadata": {"owner": OWNER},
        "compute_kind": "Validation",
    }


def _gb_transformation_settings(
    asset_name: str, input_name: str, asset_description: str = "Empty"
) -> dict:
    """Asset configuration for df transformation"""
    return {
        "name": asset_name.lower(),
        "description": asset_description,
        "ins": {input_name.lower(): AssetIn()},
        "io_manager_key": "io_manager",
        "required_resource_keys": {"local_resource", "pipes_subprocess_client"},
        "metadata": {"owner": OWNER},
        "compute_kind": "Transformation",
    }


def gb_validation(key, name, check_specs, check, description):
    """Asset factory for genbank file validation"""

    @asset(
        **_gb_validation_settings(
            asset_name=key + "_validation",
            asset_check_specs=check_specs,
            asset_description=description,
        )
    )
    # def asset_template(context: AssetExecutionContext, create_genbank_df):
    def asset_template(context: AssetExecutionContext, reload_ui_asset):
        entity = name
        fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
        path = str(Path(fs) / "gb_parsing")
        data = pl.read_parquet(f"{path}/{entity}.parquet")
        check_df = check.validate(data)

        for item in check_df.iter_rows(named=True):
            yield AssetCheckResult(
                asset_key=key.lower() + "_validation",
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

        yield Output(
            value=(data, check_df),
            metadata={
                "rows_data": len(data),
                "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                "rows_check_df": len(check_df),
                "check_df": MetadataValue.md(check_df.to_pandas().to_markdown()),
            },
        )

    return asset_template


def gb_labelling(key, input_name, description):
    """Asset factory for check evaluation and genbank file labelling"""

    @asset(
        **_gb_labelling_settings(
            asset_name=key, input_name=input_name, asset_description=description
        )
    )
    def asset_template(**kwargs):
        data, check_df = kwargs[input_name]

        # method filters
        _unique = pl.col("rule") == "is_unique"
        _complete = pl.col("rule") == "is_complete"
        # col filters
        _locus_tag = pl.col("column") == "locus_tag"
        _cds_locus_tag = pl.col("column") == "cds_locus_tag"
        _protein_id = pl.col("column") == "protein_id"
        _gene = pl.col("column") == "gene"

        # Logic
        if (
            check_df.filter(_unique & _locus_tag)
            .select("status")
            .to_series()
            .to_list()[0]
            == "PASS"
        ):
            if (
                check_df.filter(_complete & _locus_tag)
                .select("status")
                .to_series()
                .to_list()[0]
                == "PASS"
            ):
                gb_type = "locus_tag"
                data = data.with_columns(gb_type=pl.lit(gb_type))
                return Output(
                    value=(data, gb_type),
                    metadata={
                        "result": "The attribute `locus_tag` in the `gene` features will be used as unique identifier for downstream processing.",
                        "gb_type": gb_type,
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )

            elif check_df.filter(_complete & _locus_tag).select(
                "violations"
            ).to_series().to_list()[0] <= (
                check_df.filter(_complete & _cds_locus_tag)
                .select("violations")
                .to_series()
                .to_list()[0]
                | check_df.filter(_complete & _protein_id)
                .select("violations")
                .to_series()
                .to_list()[0]
            ):
                gb_type = "locus_tag"
                data = data.with_columns(gb_type=pl.lit(gb_type))
                return Output(
                    value=(data, gb_type),
                    metadata={
                        "result": "The attribute `locus_tag` in the `gene` features will be used as unique identifier for downstream processing.",
                        "gb_type": gb_type,
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )

            else:
                "Genbank file is not from type `gene`."

        elif (
            check_df.filter(_unique & _cds_locus_tag)
            .select("status")
            .to_series()
            .to_list()[0]
            == "PASS"
        ):

            if (
                check_df.filter(_complete & _cds_locus_tag)
                .select("status")
                .to_series()
                .to_list()[0]
                == "PASS"
            ):
                gb_type = "cds_locus_tag"
                data = data.with_columns(gb_type=pl.lit(gb_type))
                return Output(
                    value=(data, gb_type),
                    metadata={
                        "result": "The attribute `locus_tag` in the `cds` features will be used as unique identifier for downstream processing.",
                        "gb_type": gb_type,
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )

            elif check_df.filter(_complete & _cds_locus_tag).select(
                "violations"
            ).to_series().to_list()[0] <= (
                check_df.filter(_complete & _locus_tag)
                .select("violations")
                .to_series()
                .to_list()[0]
                | check_df.filter(_complete & _protein_id)
                .select("violations")
                .to_series()
                .to_list()[0]
            ):
                gb_type = "cds_locus_tag"
                data = data.with_columns(gb_type=pl.lit(gb_type))
                return Output(
                    value=(data, gb_type),
                    metadata={
                        "result": "The attribute `locus_tag` in the `cds` features will be used as unique identifier for downstream processing.",
                        "gb_type": gb_type,
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )

            else:
                "Genbank file is not from type `cds_locus_tag`."

        elif (
            check_df.filter(_unique & _protein_id)
            .select("status")
            .to_series()
            .to_list()[0]
            == "PASS"
        ):

            if (
                check_df.filter(_complete & _protein_id)
                .select("status")
                .to_series()
                .to_list()[0]
                == "PASS"
            ):
                gb_type = "protein_id"
                data = data.with_columns(gb_type=pl.lit(gb_type))
                return Output(
                    value=(data, gb_type),
                    metadata={
                        "result": "The attribute `protein_id` in the `cds` features will be used as unique identifier for downstream processing.",
                        "gb_type": gb_type,
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )

            elif check_df.filter(_complete & _protein_id).select(
                "violations"
            ).to_series().to_list()[0] <= (
                check_df.filter(_complete & _locus_tag)
                .select("violations")
                .to_series()
                .to_list()[0]
                | check_df.filter(_complete & _cds_locus_tag)
                .select("violations")
                .to_series()
                .to_list()[0]
            ):
                gb_type = "protein_id"
                data = data.with_columns(gb_type=pl.lit(gb_type))
                return Output(
                    value=(data, gb_type),
                    metadata={
                        "result": "The attribute `protein_id` in the `cds` features will be used as unique identifier for downstream processing.",
                        "gb_type": gb_type,
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )

            else:
                "Genbank file is not from type `protein`."

        elif (
            check_df.filter(_unique & _gene).select("status").to_series().to_list()[0]
            == "PASS"
        ):

            if (
                check_df.filter(_complete & _gene)
                .select("status")
                .to_series()
                .to_list()[0]
                == "PASS"
            ):
                gb_type = "gene"
                data = data.with_columns(gb_type=pl.lit(gb_type))
                return Output(
                    value=(data, gb_type),
                    metadata={
                        "result": "The attribute `gene` in the `gene` features will be used as unique identifier for downstream processing.",
                        "gb_type": gb_type,
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )

            elif check_df.filter(_complete & _gene).select(
                "violations"
            ).to_series().to_list()[0] <= (
                check_df.filter(_complete & _locus_tag)
                .select("violations")
                .to_series()
                .to_list()[0]
                | check_df.filter(_complete & _protein_id)
                .select("violations")
                .to_series()
                .to_list()[0]
            ):
                gb_type = "gene"
                data = data.with_columns(gb_type=pl.lit(gb_type))
                return Output(
                    value=(data, gb_type),
                    metadata={
                        "result": "The attribute `gene` in the `gene` features will be used as unique identifier for downstream processing.",
                        "gb_type": gb_type,
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )

            else:
                "Genbank file is not from type `gene`."

        else:
            "This file is too incomplete and will not be processed!"
            gb_type = None
            return Output(
                value=(data, gb_type),
                metadata={
                    "result": "None of the excpecting attribute are unique and the file cannot be processed downstream.",
                    "gb_type": gb_type,
                    "rows_data": len(data),
                    "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                },
            )

        # Check first LOCUS_TAG:
        # is_unique ?
        #   yes -> continue
        #   no -> next loop
        # is_complete AND is_unique --> 'gene' type
        # is_complete has violations but nbr violations <= nbr violations (cds_locus_tag OR protein_id)  AND is_unique --> 'locus_tag' type
        # --> downstream processing:
        #       - if protein_id, filter (locus_tag AND protein_id) isNotNull
        #       - elif cds_locus_tag, filter (locus_tag AND cds_locus_tag) isNotNull
        #       - else: filter locus_tag isNotNull
        # Check second CDS_LOCUS_TAG:
        # is_unique ?
        #   yes -> continue
        #   no -> next loop
        # is_complete AND is_unique --> 'cds_locus_tag' type
        # is_complete has violations but nbr violations <= nbr violations (locus_tag OR protein_id)  AND is_unique --> 'cds_locus_tag' type
        # --> downstream processing:
        #       filter cds_locus_tag isNotNull
        #       fill locus_tag with cds_locus_tag
        # Check third PROTEIN_ID:
        # is_unique ?
        #   yes -> continue
        #   no -> next loop
        # is_complete AND is_unique --> 'protein_id' type
        # is_complete has violations but nbr violations <= nbr violations (locus_tag OR cds_locus_tag)  AND is_unique --> 'protein' type
        # --> downstream processing:
        #       filter protein_id isNotNull
        #       fill locus tag
        # Check forth GENE:
        # is_complete AND is_unique --> 'gene' type
        # is_complete has violations but nbr violations <= nbr violations (cds_locus_tag OR protein_id)  AND is_unique --> 'gene' type
        # --> downstream processing:
        #       - if protein_id, filter (gene AND protein_id) isNotNull
        #       - elif cds_locus_tag, filter (locus_tag AND cds_locus_tag) isNotNull
        #       - else: filter locus_tag isNotNull

    return asset_template


def df_transformation(key, input_name, description):
    """Asset factory for dataframe transformation for downstream use"""

    @asset(
        **_gb_transformation_settings(
            asset_name=key, input_name=input_name, asset_description=description
        )
    )
    def asset_template(context, **kwargs):
        cmd = [shutil.which("python"), file_relative_path(__file__, "external_code.py")]
        data, gb_type = kwargs[input_name]
        entity = key
        fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
        _path = str(Path(fs) / "transformed_dfs")
        os.makedirs(_path, exist_ok=True)

        conn = duckdb.connect(":memory:")

        (
            conn.execute(
                """
                    CREATE or REPLACE TABLE genbank (
                    cds_gene string, cds_locus_tag string, protein_id string, function string, product string, translation string, transl_table string, codon_start string,
                    start_sequence integer, end_sequence integer, strand integer, cds_extract string, gene string, locus_tag string, extract string, translation_fn string, id string, name string, description string, topology string, organism string, 
                    taxonomy varchar[], filename string, gb_type string);"""
            )
        )

        parquet_destination = f"{_path}/{entity}.parquet"

        if gb_type == "locus_tag":
            if not data.select(pl.col("protein_id")).is_empty():
                data = data.filter(
                    (pl.col("locus_tag").is_not_null())
                    & (pl.col("protein_id").is_not_null())
                )

                (
                    conn.execute(
                        f"INSERT INTO genbank by position (select * from data)"
                    )
                    .execute("select * from genbank")
                    .pl()
                ).write_parquet(parquet_destination)

                context.resources.pipes_subprocess_client.run(
                    command=cmd, context=context
                )

                return Output(
                    value="ok",
                    metadata={
                        "text": "The file has been processed",
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )
            elif not data.select(pl.col("cds_locus_tag")).is_empty():
                data = data.filter(
                    (pl.col("locus_tag").is_not_null())
                    & (pl.col("cds_locus_tag").is_not_null())
                )

                (
                    conn.execute(
                        f"INSERT INTO genbank by position (select * from data)"
                    )
                    .execute("select * from genbank")
                    .pl()
                ).write_parquet(parquet_destination)

                context.resources.pipes_subprocess_client.run(
                    command=cmd, context=context
                )

                return Output(
                    value="ok",
                    metadata={
                        "text": "The file has been processed",
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )
            else:
                data = data.filter(pl.col("locus_tag").is_not_null())

                (
                    conn.execute(
                        f"INSERT INTO genbank by position (select * from data)"
                    )
                    .execute("select * from genbank")
                    .pl()
                ).write_parquet(parquet_destination)

                context.resources.pipes_subprocess_client.run(
                    command=cmd, context=context
                )

                return Output(
                    value="ok",
                    metadata={
                        "text": "The file has been processed",
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )
        elif gb_type == "cds_locus_tag":
            data = data.filter(pl.col("cds_locus_tag").is_not_null()).with_columns(
                pl.coalesce(["locus_tag", "cds_locus_tag"]).alias("locus_tag"),
                pl.coalesce(["extract", "cds_extract"]).alias("extract"),
                pl.col("cds_extract")
                .map_elements(
                    lambda x: translate(x, stop_symbol="", table=11),
                    return_dtype=pl.String,
                    skip_nulls=True,
                )
                .alias("translation_fn"),
            )

            (
                conn.execute(f"INSERT INTO genbank by position (select * from data)")
                .execute("select * from genbank")
                .pl()
            ).write_parquet(parquet_destination)

            context.resources.pipes_subprocess_client.run(command=cmd, context=context)

            return Output(
                value="ok",
                metadata={
                    "text": "The file has been processed",
                    "rows_data": len(data),
                    "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                },
            )
        elif gb_type == "protein_id":
            data = data.filter(pl.col("protein_id").is_not_null()).with_columns(
                pl.coalesce(["locus_tag", "protein_id"]).alias("locus_tag"),
                pl.coalesce(["extract", "cds_extract"]).alias("extract"),
                pl.col("cds_extract")
                .map_elements(
                    lambda x: translate(x, stop_symbol="", table=11),
                    return_dtype=pl.String,
                    skip_nulls=True,
                )
                .alias("translation_fn"),
            )

            (
                conn.execute(f"INSERT INTO genbank by position (select * from data)")
                .execute("select * from genbank")
                .pl()
            ).write_parquet(parquet_destination)

            context.resources.pipes_subprocess_client.run(command=cmd, context=context)

            return Output(
                value="ok",
                metadata={
                    "text": "The file has been processed",
                    "rows_data": len(data),
                    "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                },
            )
        elif gb_type == "gene":
            if not data.select(pl.col("protein_id")).is_empty():
                data = data.filter(
                    (pl.col("gene").is_not_null())
                    & (pl.col("protein_id").is_not_null())
                ).with_columns(pl.coalesce(["locus_tag", "gene"]).alias("locus_tag"))

                (
                    conn.execute(
                        f"INSERT INTO genbank by position (select * from data)"
                    )
                    .execute("select * from genbank")
                    .pl()
                ).write_parquet(parquet_destination)

                context.resources.pipes_subprocess_client.run(
                    command=cmd, context=context
                )

                return Output(
                    value="ok",
                    metadata={
                        "text": "The file has been processed",
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )
            elif not data.select(pl.col("cds_locus_tag")).is_empty():
                data = data.filter(
                    (pl.col("gene").is_not_null())
                    & (pl.col("cds_locus_tag").is_not_null())
                ).with_columns(pl.coalesce(["locus_tag", "gene"]).alias("locus_tag"))

                (
                    conn.execute(
                        f"INSERT INTO genbank by position (select * from data)"
                    )
                    .execute("select * from genbank")
                    .pl()
                ).write_parquet(parquet_destination)

                context.resources.pipes_subprocess_client.run(
                    command=cmd, context=context
                )

                return Output(
                    value="ok",
                    metadata={
                        "text": "The file has been processed",
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )
            else:
                data = data.filter(pl.col("gene").is_not_null()).with_columns(
                    pl.coalesce(["locus_tag", "gene"]).alias("locus_tag")
                )

                (
                    conn.execute(
                        f"INSERT INTO genbank by position (select * from data)"
                    )
                    .execute("select * from genbank")
                    .pl()
                ).write_parquet(parquet_destination)

                context.resources.pipes_subprocess_client.run(
                    command=cmd, context=context
                )

                return Output(
                    value="ok",
                    metadata={
                        "text": "The file has been processed",
                        "rows_data": len(data),
                        "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                    },
                )
        else:

            context.resources.pipes_subprocess_client.run(command=cmd, context=context)

            return Output(
                value="Not ok",
                metadata={
                    "text": "The file has not been processed. Please refer to the validation step.",
                    "rows_data": len(data),
                    "df": MetadataValue.md(data.to_pandas().head().to_markdown()),
                },
            )

        # LOGIC TEMPLATE
        # Check first LOCUS_TAG:
        # is_unique ?
        #   yes -> continue
        #   no -> next loop
        # is_complete AND is_unique --> 'gene' type
        # is_complete has violations but nbr violations <= nbr violations (cds_locus_tag OR protein_id)  AND is_unique --> 'gene' type
        # --> downstream processing:
        #       - if protein_id, filter (locus_tag AND protein_id) isNotNull
        #       - elif cds_locus_tag, filter (locus_tag AND cds_locus_tag) isNotNull
        #       - else: filter locus_tag isNotNull
        # Check second CDS_LOCUS_TAG:
        # is_unique ?
        #   yes -> continue
        #   no -> next loop
        # is_complete AND is_unique --> 'cds_locus_tag' type
        # is_complete has violations but nbr violations <= nbr violations (locus_tag OR protein_id)  AND is_unique --> 'cds_locus_tag' type
        # --> downstream processing:
        #       filter cds_locus_tag isNotNull
        #       fill locus_tag with cds_locus_tag
        # Check third PROTEIN_ID:
        # is_unique ?
        #   yes -> continue
        #   no -> next loop
        # is_complete AND is_unique --> 'protein_id' type
        # is_complete has violations but nbr violations <= nbr violations (locus_tag OR cds_locus_tag)  AND is_unique --> 'protein' type
        # --> downstream processing:
        #       filter protein_id isNotNull
        #       fill locus tag
        # Check forth GENE:
        # is_complete AND is_unique --> 'gene' type
        # is_complete has violations but nbr violations <= nbr violations (cds_locus_tag OR protein_id)  AND is_unique --> 'gene' type
        # --> downstream processing:
        #       - if protein_id, filter (gene AND protein_id) isNotNull
        #       - elif cds_locus_tag, filter (locus_tag AND cds_locus_tag) isNotNull
        #       - else: filter locus_tag isNotNull

    return asset_template


def load_dynamic():
    """Asset factory generator based on the genbank files history"""
    # fs = context.resources.local_resource.get_paths()["FILESYSTEM_DIR"]
    # path = str(Path(fs) / "gb_parsing")
    # _file = 'temp/development/data/fs/genbank_history'
    # entities = pickle.load(open(path, 'rb'))
    # asset_names = entities.history
    # path = PATH_TO_LOCAL_DIR
    # path = PATH_TO_DVP_DIR
    path = PATH_TO_LOCAL_DIR
    if os.path.exists(path):
        # asset_names = [Path(file).stem for file in os.listdir(path)]
        entities = pickle.load(open(path, "rb"))
        asset_names = [Path(e).stem for e in entities.history]
    else:
        asset_names = []
    assets = []
    for asset_name in asset_names:
        checks, check_specs = _create_check_specs(
            GENBANK_CHECKS, asset_name=asset_name.lower() + "_validation"
        )
        assets.append(
            gb_validation(
                key=asset_name,
                name=asset_name,
                check_specs=check_specs,
                check=checks,
                description=f"Load table for {asset_name} and apply the checks",
            )
        )

        assets.append(
            gb_labelling(
                key=asset_name,
                input_name=asset_name.lower() + "_validation",
                description=f"Evaluate check results for {asset_name} and label genbank file for downstream processing",
            )
        )

        assets.append(
            df_transformation(
                key=asset_name.lower() + "_transformation",
                input_name=asset_name.lower(),
                description=f"Apply transformation onto {asset_name} dataframe for downstream computation",
            )
        )

    return assets


my_dynamic_assets = load_dynamic()


# @asset
# def reload_ui_downstream_assets(
#     context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient, my_dynamic_assets
# ) -> Output:
#     # Command to reload the UI
#     cmd = [shutil.which("python"), file_relative_path(__file__, "external_code.py")]
#     # return pipes_subprocess_client.run(
#     #     command=cmd, context=context
#     # ).get_materialize_result()
#     pipes_subprocess_client.run(
#         command=cmd, context=context
#     )
#     return Output(value = "Definitions have been reloaded")
