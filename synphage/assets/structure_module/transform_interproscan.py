from dagster import asset

import polars as pl

from pathlib import Path
from synphage.resources.local_resource import OWNER


def _filter_overlapping_domains(group: pl.DataFrame) -> pl.DataFrame:
    """
    For a single protein (group), keep only non-overlapping domains.
    Domains are sorted by score ascending (best = lowest e-value first).
    A candidate domain is dropped if it overlaps >50% of the shorter domain
    with any already-kept domain.
    """
    rows = group.sort("score").to_dicts()
    kept: list[dict] = []

    for row in rows:
        overlapping = False
        for k in kept:
            # A null start/stop/length (from a failed numeric cast) cannot
            # overlap meaningfully — treat as non-overlapping, matching the
            # previous NaN-comparison behaviour.
            if None in (
                row["start"],
                row["stop"],
                k["start"],
                k["stop"],
                row["d1_length"],
                k["d1_length"],
            ):
                continue
            overlap_len = max(
                0, min(row["stop"], k["stop"]) - max(row["start"], k["start"])
            )
            min_len = min(row["d1_length"], k["d1_length"])
            if min_len > 0 and (overlap_len / min_len) > 0.5:
                overlapping = True
                break
        if not overlapping:
            kept.append(row)

    if not kept:
        return group.clear()
    return pl.DataFrame(kept, schema=group.schema)


# Output schema of _parse_interpro_tsv (also the empty-result schema). 'score' is
# used only for ranking and is intentionally not part of the output.
_PARSED_SCHEMA: dict[str, pl.DataType] = {
    "key": pl.Utf8,
    "domain": pl.Utf8,
    "d1_length": pl.Int64,
    "domain_2": pl.Utf8,
    "d2_length": pl.Int64,
    "domain_3": pl.Utf8,
    "d3_length": pl.Int64,
    "n_domains": pl.UInt32,
}

# The 13 InterProScan TSV columns we read. GO-term / pathway columns (14-15) are
# disabled at submission, so we read only the first 13 to stay robust to their
# presence or absence.
_TSV_COLUMNS = [
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
]


def _empty_parsed_frame() -> pl.DataFrame:
    """An empty but well-structured parsed-domain DataFrame."""
    return pl.DataFrame(schema=_PARSED_SCHEMA)


def _parse_interpro_tsv(tsv_path: Path) -> pl.DataFrame:
    """
    Read the cumulative InterProScan TSV and return a tidy per-protein DataFrame
    with columns: key, domain, d1_length, domain_2, d2_length, domain_3,
                  d3_length, n_domains.

    The 'key' field is the InterProScan protein accession, taken from the FASTA
    header written by create_fasta_p (">%s \n" % key). The trailing space in the
    header terminates the accession token, so the API already reports a clean key.
    """
    interpro = pl.read_csv(
        tsv_path,
        separator="\t",
        has_header=False,
        columns=list(range(13)),
        new_columns=_TSV_COLUMNS,
        infer_schema_length=0,
        truncate_ragged_lines=True,
    )

    interpro = (
        interpro.select(["key", "start", "stop", "score", "domain"])
        .with_columns(
            pl.col("start").cast(pl.Int64, strict=False),
            pl.col("stop").cast(pl.Int64, strict=False),
            pl.col("score").cast(pl.Float64, strict=False),
        )
        # Drop rows with no meaningful domain annotation
        .filter(pl.col("domain") != "-")
        # Compute domain length
        .with_columns((pl.col("stop") - pl.col("start")).alias("d1_length"))
    )

    if interpro.is_empty():
        return _empty_parsed_frame()

    # Remove overlapping domains per protein, keeping best (lowest) score
    interpro = interpro.group_by("key").map_groups(_filter_overlapping_domains)

    if interpro.is_empty():
        return _empty_parsed_frame()

    # Collect domains per protein with the best score first, then expand into
    # best / 2nd / 3rd columns. n_domains is the number of kept domains.
    grouped = (
        interpro.sort(["key", "score"])
        .group_by("key", maintain_order=True)
        .agg(
            pl.col("domain").alias("_domains"),
            pl.col("d1_length").alias("_lengths"),
        )
    )

    return grouped.select(
        "key",
        pl.col("_domains").list.get(0, null_on_oob=True).alias("domain"),
        pl.col("_lengths").list.get(0, null_on_oob=True).alias("d1_length"),
        pl.col("_domains").list.get(1, null_on_oob=True).alias("domain_2"),
        pl.col("_lengths").list.get(1, null_on_oob=True).alias("d2_length"),
        pl.col("_domains").list.get(2, null_on_oob=True).alias("domain_3"),
        pl.col("_lengths").list.get(2, null_on_oob=True).alias("d3_length"),
        pl.col("_domains").list.len().alias("n_domains"),
    )


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
    chunks_dir = Path(
        context.resources.local_resource.get_paths()["INTERPRO_CHUNKS_DIR"]
    )

    final_tsv = interpro_dir / "interproscan_results.tsv"
    new_chunk_paths: list[str] = run_interpro_scan

    # ------------------------------------------------------------------ #
    # Step 1 — (re)build the cumulative results TSV from every chunk TSV  #
    #          currently in the chunks directory.                         #
    #                                                                     #
    # Rebuilding (rather than appending the upstream list) is idempotent: #
    # re-materializing the asset can never duplicate rows, and a TSV that #
    # was previously bloated by repeated appends self-heals on the next   #
    # run. run_interpro_scan is still consumed as the upstream dependency #
    # and is reported as the number of chunks touched this run.           #
    # ------------------------------------------------------------------ #
    chunk_tsvs = sorted(chunks_dir.glob("*.tsv")) if chunks_dir.exists() else []
    context.log.info(
        f"Rebuilding {final_tsv.name} from {len(chunk_tsvs)} chunk TSV(s) in {chunks_dir}"
    )

    lines_written = 0
    with final_tsv.open("w", encoding="utf-8") as outfile:
        for chunk in chunk_tsvs:
            content = chunk.read_text(encoding="utf-8")
            if content and not content.endswith("\n"):
                content += "\n"
            outfile.write(content)
            lines_written += content.count("\n")
    context.log.info(
        f"Rebuild complete. {lines_written} line(s) across {len(chunk_tsvs)} "
        f"chunk(s) written to {final_tsv.name}."
    )

    # ------------------------------------------------------------------ #
    # Step 2 — parse cumulative TSV                                       #
    # ------------------------------------------------------------------ #
    if lines_written == 0:
        context.log.warning(
            f"No InterProScan results available in {chunks_dir}; "
            "producing an empty annotation table."
        )
        interpro_parsed = _empty_parsed_frame()
    else:
        context.log.info(f"Parsing cumulative InterProScan TSV: {final_tsv}")
        interpro_parsed = _parse_interpro_tsv(final_tsv)
    context.log.info(
        f"Parsed {interpro_parsed.height} unique proteins with domain annotations"
    )

    # ------------------------------------------------------------------ #
    # Step 3 — merge with processed_genbank_df                           #
    # ------------------------------------------------------------------ #
    parquet_path = tables_dir / "processed_genbank_df.parquet"
    context.log.info(f"Loading genbank DataFrame from: {parquet_path}")
    genbank_df = pl.read_parquet(parquet_path).with_columns(
        pl.col("key").cast(pl.Utf8)
    )

    df = genbank_df.join(interpro_parsed, on="key", how="left")
    unmatched = df.filter(pl.col("domain").is_null()).height
    context.log.info(
        f"After merge: {df.height} rows, {unmatched} rows without domain annotation"
    )

    # ------------------------------------------------------------------ #
    # Step 4 — write output parquet                                       #
    # ------------------------------------------------------------------ #
    output_path = tables_dir / "genbank_df_interpro.parquet"
    df.write_parquet(output_path)
    context.log.info(f"InterPro DataFrame written to: {output_path}")

    context.add_output_metadata(
        metadata={
            "output_tsv": str(final_tsv),
            "output_parquet": str(output_path),
            "new_chunks_this_run": len(new_chunk_paths),
            "total_chunks": len(chunk_tsvs),
            "lines_written": lines_written,
            "num_rows": df.height,
            "proteins_with_domains": interpro_parsed.height,
            "unmatched_rows": unmatched,
        }
    )

    return str(output_path)
