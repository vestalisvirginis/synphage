from dagster import asset

import pandas as pd

from pathlib import Path

from synphage.resources.local_resource import OWNER


@asset(
    required_resource_keys={"local_resource"},
    description=(
        "Merges the processed genbank DataFrame with Foldseek cluster results. "
        "Adds 'representative_key' (cluster representative), 'cluster_size' (number of "
        "members per cluster), and 'representative' (protein_id of the cluster representative). "
        "Outputs 'genbank_df_clustered.parquet' to the tables directory."
    ),
    compute_kind="Python",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def transform_foldseek(context, run_foldseek_cluster) -> str:
    tables_dir = Path(context.resources.local_resource.get_paths()["TABLES_DIR"])

    # Load the main genbank DataFrame; cast key to str for safe merging
    parquet_path = tables_dir / "processed_genbank_df.parquet"
    context.log.info(f"Loading genbank DataFrame from: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    df["key"] = df["key"].astype(str)

    # Load Foldseek cluster TSV using the path returned by the upstream asset.
    # dtype=str prevents pandas from inferring integer types on hash-based keys.
    cluster_tsv_path = Path(run_foldseek_cluster)
    context.log.info(f"Loading Foldseek cluster TSV from: {cluster_tsv_path}")
    cluster_df = pd.read_csv(
        cluster_tsv_path,
        sep="\t",
        header=None,
        names=["representative_key", "key"],
        dtype=str,
    )

    # Strip the f_ prefix added during FASTA generation so keys align with
    # the raw integer-string key values stored in the genbank DataFrame.
    cluster_df["key"] = cluster_df["key"].str.removeprefix("f_")
    cluster_df["representative_key"] = cluster_df[
        "representative_key"
    ].str.removeprefix("f_")

    context.log.info(
        f"Cluster TSV: {len(cluster_df)} rows, "
        f"{cluster_df['representative_key'].nunique()} unique clusters"
    )

    # Left join: every genbank row gets a representative_key where matched
    df = df.merge(cluster_df, on="key", how="left")
    unmatched = int(df["representative_key"].isna().sum())
    context.log.info(f"After merge: {len(df)} rows, {unmatched} unmatched")

    # cluster_size: number of members sharing the same representative
    df["cluster_size"] = df.groupby("representative_key")["key"].transform("count")

    # representative: protein_id of the row whose key == representative_key
    key_to_protein_id = df.set_index("key")["protein_id"]
    df["representative"] = df["representative_key"].map(key_to_protein_id)

    # Write output
    output_path = tables_dir / "genbank_df_clustered.parquet"
    df.to_parquet(output_path, index=False)
    context.log.info(f"Clustered DataFrame written to: {output_path}")

    context.add_output_metadata(
        metadata={
            "output_path": str(output_path),
            "num_rows": len(df),
            "num_clusters": int(df["representative_key"].nunique()),
            "unmatched_rows": unmatched,
        }
    )

    return str(output_path)
