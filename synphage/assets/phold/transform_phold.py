from dagster import asset

import pandas as pd

from pathlib import Path

from synphage.resources.local_resource import OWNER

_PHOLD_COLUMNS = [
    "cds_id",
    "phrog",
    "function",
    "product",
    "annotation_confidence",
    "bitscore",
    "fident",
    "evalue",
    "tophit_protein",
    "prostt5_confidence",
]

_RENAME_MAP = {
    "function": "function_phold",
    "product": "product_phold",
}


@asset(
    required_resource_keys={"local_resource"},
    description=(
        "Merges the processed genbank DataFrame with Phold per-CDS annotation results. "
        "Adds phrog, function_phold, product_phold, annotation_confidence, bitscore, "
        "fident, evalue, tophit_protein, and prostt5_confidence columns from the phold "
        "per-CDS predictions TSV. Outputs 'genbank_df_phold_annotated.parquet' to the "
        "tables directory."
    ),
    compute_kind="Python",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def transform_phold(context, run_phold_annotation: str) -> str:
    tables_dir = Path(context.resources.local_resource.get_paths()["TABLES_DIR"])

    # Load the main genbank DataFrame; cast key to str for safe merging
    parquet_path = tables_dir / "processed_genbank_df.parquet"
    context.log.info(f"Loading genbank DataFrame from: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    df["key"] = df["key"].astype(str)

    # Load phold per-CDS TSV from the directory returned by the upstream asset
    tsv_path = (
        Path(run_phold_annotation) / "annotations" / "phold_per_cds_predictions.tsv"
    )
    context.log.info(f"Loading phold annotations TSV from: {tsv_path}")
    phold_df = pd.read_csv(tsv_path, sep="\t", dtype=str)

    context.log.info(
        f"Phold TSV: {len(phold_df)} rows, "
        f"{phold_df['cds_id'].nunique()} unique CDS IDs"
    )

    # Select only the required columns and rename colliding ones
    phold_df = phold_df[_PHOLD_COLUMNS].rename(columns=_RENAME_MAP)

    # Strip the "p_" prefix added during FASTA generation so cds_id aligns
    # with the raw key values stored in the genbank DataFrame
    phold_df["cds_id"] = phold_df["cds_id"].str.removeprefix("p_")

    # Left join: every genbank row gets phold annotations where matched
    df = df.merge(phold_df, left_on="key", right_on="cds_id", how="left")
    unmatched = int(df["phrog"].isna().sum())
    context.log.info(f"After merge: {len(df)} rows, {unmatched} unmatched")

    # Write output to the same tables directory as the input parquet
    output_path = tables_dir / "genbank_df_phold_annotated.parquet"
    df.to_parquet(output_path, index=False)
    context.log.info(f"Phold-annotated DataFrame written to: {output_path}")

    context.add_output_metadata(
        metadata={
            "output_path": str(output_path),
            "num_rows": len(df),
            "unmatched_rows": unmatched,
        }
    )

    return str(output_path)
