from dagster import asset, AssetCheckResult, AssetCheckSpec, AssetKey, Config, Output

import zipfile

from pathlib import Path

from synphage.resources.local_resource import OWNER
from synphage.assets.structure_module.foldseek_modal import (
    app,
    foldseek_volume,
    run_foldseek_cluster_remote,
    DEFAULT_CLUSTER_EVALUE,
)


class FoldseekClusterConfig(Config):  # type: ignore[misc]
    """Run-time knobs for the Foldseek clustering asset."""

    # easy-cluster e-value threshold passed to Foldseek on the remote GPU worker.
    evalue: float = DEFAULT_CLUSTER_EVALUE


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
    check_specs=[
        AssetCheckSpec(
            name="cluster_covers_all_inputs",
            asset="run_foldseek_cluster",
            description=(
                "Foldseek easy-cluster must assign every input protein to a "
                "cluster: the cluster TSV row count must equal the number of "
                "sequences in the input FASTA. A shortfall indicates Foldseek "
                "silently dropped sequences."
            ),
        ),
    ],
)
def run_foldseek_cluster(context, config: FoldseekClusterConfig, create_fasta_p):
    _foldseek_dir = Path(context.resources.local_resource.get_paths()["FOLDSEEK_DIR"])
    _foldseek_dir.mkdir(parents=True, exist_ok=True)

    fasta_files = create_fasta_p.history
    context.log.info(
        f"Concatenating {len(fasta_files)} FASTA file(s) for Foldseek input"
    )

    # Stream-concatenate every FASTA into a single staging file on disk rather
    # than holding the whole protein corpus in memory as one string. This file is
    # uploaded to the Modal volume and read by the remote worker.
    staging_fasta = _foldseek_dir / "foldseek_input.fasta"
    num_fasta_files = 0
    with staging_fasta.open("w", encoding="utf-8") as out_f:
        for fasta_path in fasta_files:
            p = Path(fasta_path)
            if not p.exists():
                context.log.warning(f"FASTA file not found, skipping: {fasta_path}")
                continue
            content = p.read_text(encoding="utf-8")
            out_f.write(content)
            if content and not content.endswith("\n"):
                out_f.write("\n")
            num_fasta_files += 1
            context.log.info(f"Loaded: {p.name}")

    context.log.info(
        f"Staged {num_fasta_files} FASTA file(s) "
        f"({staging_fasta.stat().st_size} bytes) to {staging_fasta}"
    )

    input_name = "foldseek_input.fasta"
    output_name = "foldseek_results.zip"
    zip_out_path = _foldseek_dir / output_name

    context.log.info(
        f"Dispatching to Modal for Foldseek clustering (e-value={config.evalue}) ..."
    )
    with app.run():
        # Upload the staged FASTA to the shared volume, run the remote job, then
        # stream the results ZIP back down from the volume.
        with foldseek_volume.batch_upload(force=True) as batch:
            batch.put_file(str(staging_fasta), input_name)

        run_foldseek_cluster_remote.remote(
            input_name, output_name, evalue=config.evalue
        )

        with zip_out_path.open("wb") as out_f:
            for chunk in foldseek_volume.read_file(output_name):
                out_f.write(chunk)

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

    # Validation: every input sequence should appear as a cluster member. Count
    # FASTA records ('>' headers) against rows in the cluster TSV.
    input_seq_count = sum(
        1
        for line in staging_fasta.read_text(encoding="utf-8").splitlines()
        if line.startswith(">")
    )
    cluster_member_count = sum(
        1
        for line in output_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    )
    coverage_passed = input_seq_count == cluster_member_count
    if not coverage_passed:
        context.log.warning(
            f"Cluster coverage mismatch: {input_seq_count} input sequence(s) but "
            f"{cluster_member_count} cluster member row(s)."
        )

    yield Output(
        value=str(output_path),
        metadata={
            "output_path": str(output_path),
            "zip_path": str(zip_out_path),
            "num_fasta_files": num_fasta_files,
            "fasta_files": fasta_files,
            "zip_size_bytes": zip_out_path.stat().st_size,
            "input_sequences": input_seq_count,
            "cluster_members": cluster_member_count,
        },
    )

    yield AssetCheckResult(
        check_name="cluster_covers_all_inputs",
        passed=coverage_passed,
        metadata={
            "input_sequences": input_seq_count,
            "cluster_members": cluster_member_count,
        },
    )
