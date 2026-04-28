from dagster import asset, AssetKey

import zipfile

from pathlib import Path

from synphage.resources.local_resource import OWNER
from synphage.assets.foldseek.foldseek_modal import app, run_foldseek_cluster_remote


@asset(
    deps=[AssetKey("transform_blastn"), AssetKey("transform_blastp")],
    required_resource_keys={"local_resource"},
    description=(
        "Concatenates all protein FASTA files and runs structure-based clustering "
        "via Foldseek on Modal GPU infrastructure. Outputs a cluster TSV file."
    ),
    compute_kind="Modal",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def run_foldseek_cluster(context, create_fasta_p) -> str:
    _foldseek_dir = Path(context.resources.local_resource.get_paths()["FOLDSEEK_DIR"])
    _foldseek_dir.mkdir(parents=True, exist_ok=True)

    fasta_files = create_fasta_p.history
    context.log.info(
        f"Concatenating {len(fasta_files)} FASTA file(s) for Foldseek input"
    )

    fasta_parts: list[str] = []
    for fasta_path in fasta_files:
        p = Path(fasta_path)
        if p.exists():
            fasta_parts.append(p.read_text(encoding="utf-8"))
            context.log.info(f"Loaded: {p.name}")
        else:
            context.log.warning(f"FASTA file not found, skipping: {fasta_path}")

    fasta_content = "\n".join(fasta_parts)
    context.log.info(
        f"Total FASTA content: {len(fasta_content)} characters across {len(fasta_parts)} file(s)"
    )

    context.log.info("Dispatching to Modal for Foldseek clustering ...")
    with app.run():
        results_zip_bytes = run_foldseek_cluster_remote.remote(fasta_content)

    # Save the returned ZIP
    zip_out_path = _foldseek_dir / "foldseek_results.zip"
    zip_out_path.write_bytes(results_zip_bytes)
    context.log.info(f"Foldseek results ZIP written to: {zip_out_path}")

    # Extract ZIP to FOLDSEEK_DIR
    with zipfile.ZipFile(zip_out_path, "r") as zip_ref:
        zip_ref.extractall(_foldseek_dir)
    context.log.info(f"Foldseek results extracted to: {_foldseek_dir}")

    # Locate results_cluster.tsv inside the extracted directory
    tsv_matches = list(_foldseek_dir.rglob("results_cluster.tsv"))
    if not tsv_matches:
        raise FileNotFoundError(
            f"results_cluster.tsv not found after extracting ZIP to {_foldseek_dir}"
        )
    output_path = tsv_matches[0]
    context.log.info(f"Foldseek cluster TSV located at: {output_path}")

    context.add_output_metadata(
        metadata={
            "output_path": str(output_path),
            "zip_path": str(zip_out_path),
            "num_fasta_files": len(fasta_parts),
            "fasta_files": fasta_files,
            "zip_size_bytes": len(results_zip_bytes),
        }
    )

    return str(output_path)
