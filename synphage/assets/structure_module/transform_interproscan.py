from dagster import asset

from pathlib import Path
from synphage.resources.local_resource import OWNER


@asset(
    required_resource_keys={"local_resource"},
    description=(
        "Appends newly downloaded InterProScan chunk TSV files into a single cumulative "
        "interproscan_results.tsv in the interpro output directory."
    ),
    compute_kind="Python",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def transform_interproscan(context, run_interpro_scan: list[str]) -> str:
    interpro_dir = Path(context.resources.local_resource.get_paths()["INTERPRO_DIR"])
    interpro_dir.mkdir(parents=True, exist_ok=True)

    final_tsv = interpro_dir / "interproscan_results.tsv"
    new_chunk_paths: list[str] = run_interpro_scan

    if not new_chunk_paths:
        context.log.info("No new chunk TSV files to append.")
        context.add_output_metadata(
            metadata={
                "output_tsv": str(final_tsv),
                "chunks_appended": 0,
                "lines_appended": 0,
            }
        )
        return str(final_tsv)

    lines_appended = 0
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
        f"Transform complete. {lines_appended} total line(s) appended to {final_tsv.name}."
    )

    context.add_output_metadata(
        metadata={
            "output_tsv": str(final_tsv),
            "chunks_appended": len(new_chunk_paths),
            "lines_appended": lines_appended,
        }
    )

    return str(final_tsv)
