from dagster import asset

import pandas as pd

from pathlib import Path
from synphage.resources.local_resource import OWNER


def _filter_overlapping_domains(group: pd.DataFrame) -> pd.DataFrame:
    """
    For a single protein (group), keep only non-overlapping domains.
    Domains are sorted by score ascending (best = lowest e-value first).
    A candidate domain is dropped if it overlaps >50% of the shorter domain
    with any already-kept domain.
    """
    group = group.sort_values(by="score", ascending=True).reset_index(drop=True)
    kept_idx: list[int] = []
    kept_rows: list[dict] = []

    for i, row in group.iterrows():
        is_overlapping = False
        for k in kept_rows:
            overlap_start = max(row["start"], k["start"])
            overlap_stop = min(row["stop"], k["stop"])
            overlap_len = max(0, overlap_stop - overlap_start)
            min_len = min(row["d1_length"], k["d1_length"])
            if min_len > 0 and (overlap_len / min_len) > 0.5:
                is_overlapping = True
                break
        if not is_overlapping:
            kept_idx.append(i)
            kept_rows.append(row.to_dict())

    # Use .loc on the reset-index group so column names are always preserved
    return group.loc[kept_idx].reset_index(drop=True)


def _parse_interpro_tsv(tsv_path: Path) -> pd.DataFrame:
    """
    Read the cumulative InterProScan TSV and return a tidy per-protein DataFrame
    with columns: key, domain, d1_length, score, domain_2, d2_length,
                  domain_3, d3_length, n_domains.

    The TSV 'ID' field comes from the FASTA header written by create_fasta_p
    which uses ">%s \n" % key, so IDs may carry a trailing space — stripped here.
    """
    interpro = pd.read_csv(
        tsv_path,
        sep="\t",
        names=[
            "key",
            "MD5",
            "length",
            "analysis",
            "signature_accession",
            "signature_description",
            "start",
            "stop",
            "score",
            "status",
            "date",
            "interpro_accession",
            "domain",
            "GO_annotations",
            "pathways_annotations",
        ],
        dtype=str,
    )
    print(
        f"[DEBUG] after read_csv: {len(interpro)} rows, cols: {interpro.columns.tolist()}"
    )
    print(f"[DEBUG] first 3 rows:\n{interpro.head(3).to_string()}")

    # Strip whitespace from ID (FASTA header may have trailing space)
    # interpro["ID"] = interpro["ID"].str.strip()

    # Drop columns not needed for the output
    interpro.drop(
        columns=[
            "MD5",
            "length",
            "analysis",
            "signature_accession",
            "status",
            "date",
            "interpro_accession",
            "GO_annotations",
            "pathways_annotations",
            "signature_description",
        ],
        inplace=True,
    )
    print(
        f"[DEBUG] after drop: {len(interpro)} rows, cols: {interpro.columns.tolist()}"
    )

    # Numeric coercion for positional / score columns
    for col in ("start", "stop", "score"):
        interpro[col] = pd.to_numeric(interpro[col], errors="coerce")
    print(
        f"[DEBUG] after numeric coerce — score sample: {interpro['score'].head(5).tolist()}"
    )
    print(
        f"[DEBUG] score NaN count: {interpro['score'].isna().sum()} / {len(interpro)}"
    )

    # Drop rows with no meaningful domain annotation
    interpro = interpro[interpro["domain"] != "-"].copy()
    print(f"[DEBUG] after domain filter (drop '-'): {len(interpro)} rows")
    print(f"[DEBUG] unique domains sample: {interpro['domain'].unique()[:10].tolist()}")

    # Compute domain length
    interpro["d1_length"] = interpro["stop"] - interpro["start"]
    print(f"[DEBUG] d1_length sample: {interpro['d1_length'].head(5).tolist()}")
    print(
        f"[DEBUG] d1_length NaN count: {interpro['d1_length'].isna().sum()} / {len(interpro)}"
    )

    # Remove overlapping domains per protein, keeping best (lowest) score
    print(
        f"[DEBUG] before filter: {len(interpro)} rows, unique keys: {interpro['key'].nunique()}"
    )
    groups = []
    for key, group in interpro.groupby("key"):
        filtered = _filter_overlapping_domains(group.drop(columns=["key"]))
        filtered["key"] = key
        groups.append(filtered)
    interpro = pd.concat(groups, ignore_index=True) if groups else interpro.iloc[0:0]
    print(
        f"[DEBUG] after filter: {len(interpro)} rows, cols: {interpro.columns.tolist()}"
    )

    # Guard: if all domains were filtered out, return an empty but well-structured DataFrame
    if interpro.empty or "key" not in interpro.columns:
        return pd.DataFrame(
            columns=[
                "key",
                "domain",
                "d1_length",
                "score",
                "domain_2",
                "d2_length",
                "domain_3",
                "d3_length",
                "n_domains",
            ]
        )

    # Count valid domains per protein after filtering
    domain_counts = interpro.groupby("key").size().reset_index(name="n_domains")

    # Sort so best-scoring domain is first for each protein
    interpro = interpro.sort_values(by=["key", "score"], ascending=[True, True])

    # Best domain (first row per protein)
    interpro_parsed = interpro.drop_duplicates(subset=["key"], keep="first").copy()
    interpro_parsed = pd.merge(interpro_parsed, domain_counts, on="key", how="left")

    # Second-best domain
    second_domain = (
        interpro.groupby("key")
        .nth(1)
        .reset_index()[["key", "domain", "d1_length"]]
        .rename(columns={"domain": "domain_2", "d1_length": "d2_length"})
    )

    # Third-best domain
    third_domain = (
        interpro.groupby("key")
        .nth(2)
        .reset_index()[["key", "domain", "d1_length"]]
        .rename(columns={"domain": "domain_3", "d1_length": "d3_length"})
    )

    interpro_parsed = pd.merge(interpro_parsed, second_domain, on="key", how="left")
    interpro_parsed = pd.merge(interpro_parsed, third_domain, on="key", how="left")
    interpro_parsed.drop(columns=["start", "stop", "score"], inplace=True)

    # Rename ID -> key for merging with processed_genbank_df
    # interpro_parsed = interpro_parsed.rename(columns={"ID": "key"})

    return interpro_parsed


@asset(
    required_resource_keys={"local_resource"},
    description=(
        "Appends newly downloaded InterProScan chunk TSV files into a single cumulative "
        "interproscan_results.tsv, parses domain annotations (best/2nd/3rd non-overlapping "
        "domain per protein), merges with processed_genbank_df.parquet on 'key', and writes "
        "'genbank_df_interpro.parquet' to the tables directory."
    ),
    compute_kind="Python",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def transform_interproscan(context, run_interpro_scan: list[str]) -> str:
    interpro_dir = Path(context.resources.local_resource.get_paths()["INTERPRO_DIR"])
    interpro_dir.mkdir(parents=True, exist_ok=True)

    tables_dir = Path(context.resources.local_resource.get_paths()["TABLES_DIR"])

    final_tsv = interpro_dir / "interproscan_results.tsv"
    new_chunk_paths: list[str] = run_interpro_scan

    # ------------------------------------------------------------------ #
    # Step 1 — append new chunk TSVs into the cumulative results file     #
    # ------------------------------------------------------------------ #
    lines_appended = 0
    if not new_chunk_paths:
        context.log.info("No new chunk TSV files to append.")
    else:
        context.log.info(
            f"Appending {len(new_chunk_paths)} chunk TSV(s) to {final_tsv.name}"
        )
        with final_tsv.open("a", encoding="utf-8") as outfile:
            for chunk_path in new_chunk_paths:
                chunk = Path(chunk_path)
                if not chunk.exists():
                    context.log.warning(f"Chunk TSV not found, skipping: {chunk_path}")
                    continue
                content = chunk.read_text(encoding="utf-8")
                if content and not content.endswith("\n"):
                    content += "\n"
                outfile.write(content)
                chunk_lines = content.count("\n")
                lines_appended += chunk_lines
                context.log.info(f"  Appended {chunk_lines} line(s) from {chunk.name}")
        context.log.info(
            f"Append complete. {lines_appended} total line(s) added to {final_tsv.name}."
        )

    # ------------------------------------------------------------------ #
    # Step 2 — parse cumulative TSV                                       #
    # ------------------------------------------------------------------ #
    if not final_tsv.exists():
        raise FileNotFoundError(
            f"Cumulative InterProScan TSV not found at {final_tsv}. "
            "Run run_interpro_scan first to generate results."
        )

    context.log.info(f"Parsing cumulative InterProScan TSV: {final_tsv}")
    interpro_parsed = _parse_interpro_tsv(final_tsv)
    context.log.info(
        f"Parsed {len(interpro_parsed)} unique proteins with domain annotations"
    )

    # ------------------------------------------------------------------ #
    # Step 3 — merge with processed_genbank_df                           #
    # ------------------------------------------------------------------ #
    parquet_path = tables_dir / "processed_genbank_df.parquet"
    context.log.info(f"Loading genbank DataFrame from: {parquet_path}")
    genbank_df = pd.read_parquet(parquet_path)
    genbank_df["key"] = genbank_df["key"].astype(str)

    df = genbank_df.merge(interpro_parsed, on="key", how="left")
    unmatched = int(df["domain"].isna().sum())
    context.log.info(
        f"After merge: {len(df)} rows, {unmatched} rows without domain annotation"
    )

    # ------------------------------------------------------------------ #
    # Step 4 — write output parquet                                       #
    # ------------------------------------------------------------------ #
    output_path = tables_dir / "genbank_df_interpro.parquet"
    df.to_parquet(output_path, index=False)
    context.log.info(f"InterPro DataFrame written to: {output_path}")

    context.add_output_metadata(
        metadata={
            "output_tsv": str(final_tsv),
            "output_parquet": str(output_path),
            "chunks_appended": len(new_chunk_paths),
            "lines_appended": lines_appended,
            "num_rows": len(df),
            "proteins_with_domains": len(interpro_parsed),
            "unmatched_rows": unmatched,
        }
    )

    return str(output_path)
