from dagster import asset, AssetCheckResult, AssetCheckSpec, Output

import polars as pl

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
    check_specs=[
        AssetCheckSpec(
            name="cluster_tsv_row_count_matches",
            asset="transform_foldseek",
            description="Cluster TSV must contain exactly one row per protein in the genbank DataFrame.",
        ),
        AssetCheckSpec(
            name="cluster_tsv_no_missing_values",
            asset="transform_foldseek",
            description="Cluster TSV must have no missing values in either column.",
        ),
    ],
)
def transform_foldseek(context, run_foldseek_cluster):
    tables_dir = Path(context.resources.local_resource.get_paths()["TABLES_DIR"])

    # Load the main genbank DataFrame; cast key to str for safe merging
    parquet_path = tables_dir / "processed_genbank_df.parquet"
    context.log.info(f"Loading genbank DataFrame from: {parquet_path}")
    df = pl.read_parquet(parquet_path).with_columns(pl.col("key").cast(pl.Utf8))
    genbank_row_count = df.height

    # The FASTA corpus sent to Foldseek contains one entry per protein, i.e. rows
    # that carry a translation. Gene-only / non-CDS rows in the genbank DataFrame
    # are not clustered, so the cluster TSV is expected to have one row per
    # *protein*, not per genbank row. Use the protein count as the check baseline.
    if "translation_fn" in df.columns:
        protein_count = df.filter(pl.col("translation_fn").is_not_null()).height
    else:
        protein_count = genbank_row_count

    # Load Foldseek cluster TSV using the path returned by the upstream asset.
    # infer_schema_length=0 reads all as Utf8 so hash-based keys are not coerced.
    cluster_tsv_path = Path(run_foldseek_cluster)
    context.log.info(f"Loading Foldseek cluster TSV from: {cluster_tsv_path}")
    cluster_df = pl.read_csv(
        cluster_tsv_path,
        separator="\t",
        has_header=False,
        new_columns=["representative_key", "key"],
        infer_schema_length=0,
    )

    context.log.info(
        f"Cluster TSV: {cluster_df.height} rows, "
        f"{cluster_df['representative_key'].n_unique()} unique clusters"
    )

    # Validation: one cluster TSV row per protein
    tsv_row_count = cluster_df.height
    row_count_passed = tsv_row_count == protein_count
    if not row_count_passed:
        context.log.warning(
            f"Row count mismatch: {protein_count} protein(s) in genbank DataFrame, "
            f"cluster TSV has {tsv_row_count} rows."
        )

    # Validation: completeness (no missing values in either TSV column)
    null_counts = cluster_df.null_count()
    missing_counts = {col: int(null_counts[col][0]) for col in cluster_df.columns}
    no_missing_passed = sum(missing_counts.values()) == 0
    if not no_missing_passed:
        context.log.warning(f"Missing values found in cluster TSV: {missing_counts}")

    # Left join: every genbank row gets a representative_key where matched
    df = df.join(cluster_df, on="key", how="left")
    unmatched = df.filter(pl.col("representative_key").is_null()).height
    context.log.info(f"After merge: {df.height} rows, {unmatched} unmatched")

    # cluster_size: number of members sharing the same representative (null for
    # unmatched rows, which have no representative).
    df = df.with_columns(
        pl.when(pl.col("representative_key").is_null())
        .then(None)
        .otherwise(pl.len().over("representative_key"))
        .alias("cluster_size")
    )

    # representative: protein_id of the row whose key == representative_key
    rep_lookup = df.select(["key", "protein_id"]).rename(
        {"key": "representative_key", "protein_id": "representative"}
    )
    df = df.join(rep_lookup, on="representative_key", how="left")

    # Write output
    output_path = tables_dir / "genbank_df_clustered.parquet"
    df.write_parquet(output_path)
    context.log.info(f"Clustered DataFrame written to: {output_path}")

    num_clusters = df.select(
        pl.col("representative_key").drop_nulls().n_unique()
    ).item()

    yield Output(
        value=str(output_path),
        metadata={
            "output_path": str(output_path),
            "num_rows": df.height,
            "num_clusters": int(num_clusters),
            "unmatched_rows": unmatched,
        },
    )

    yield AssetCheckResult(
        check_name="cluster_tsv_row_count_matches",
        passed=row_count_passed,
        metadata={
            "genbank_row_count": genbank_row_count,
            "protein_count": protein_count,
            "cluster_tsv_row_count": tsv_row_count,
        },
    )

    yield AssetCheckResult(
        check_name="cluster_tsv_no_missing_values",
        passed=no_missing_passed,
        metadata={
            "missing_per_column": missing_counts,
        },
    )
