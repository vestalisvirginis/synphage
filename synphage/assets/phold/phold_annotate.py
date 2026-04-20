from dagster import asset

import zipfile

from pathlib import Path

from synphage.resources.local_resource import OWNER
from synphage.assets.phold.phold_modal import app, run_phold_remote


@asset(
    required_resource_keys={"local_resource"},
    description=(
        "Concatenates all protein FASTA files and runs structure-based protein "
        "annotation via Phold on Modal GPU infrastructure. Outputs predictions "
        "(3Di tokens) and annotations (TSV) in the phold output directory."
    ),
    compute_kind="Modal",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def run_phold_annotation(context, phold_create_fasta_p) -> str:
    _phold_dir = Path(context.resources.local_resource.get_paths()["PHOLD_DIR"])
    _phold_dir.mkdir(parents=True, exist_ok=True)

    fasta_files = phold_create_fasta_p.history
    context.log.info(f"Concatenating {len(fasta_files)} FASTA file(s) for Phold input")

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

    context.log.info("Dispatching to Modal for Phold protein annotation ...")
    with app.run():
        results_zip_bytes = run_phold_remote.remote(fasta_content)

    # Save the returned ZIP
    zip_out_path = _phold_dir / "phold_results.zip"
    zip_out_path.write_bytes(results_zip_bytes)
    context.log.info(f"Phold results ZIP written to: {zip_out_path}")

    # Extract ZIP — produces predictions/ and annotations/ subdirs
    with zipfile.ZipFile(zip_out_path, "r") as zip_ref:
        zip_ref.extractall(_phold_dir)
    context.log.info(f"Phold results extracted to: {_phold_dir}")

    context.add_output_metadata(
        metadata={
            "output_dir": str(_phold_dir),
            "zip_path": str(zip_out_path),
            "num_fasta_files": len(fasta_parts),
            "fasta_files": fasta_files,
            "zip_size_bytes": len(results_zip_bytes),
        }
    )

    return str(_phold_dir)
