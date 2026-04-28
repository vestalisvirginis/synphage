from dagster import asset

import os
import time
import requests

from pathlib import Path
from synphage.resources.local_resource import OWNER

BASE_URL = "https://www.ebi.ac.uk/Tools/services/rest/iprscan5"
MAX_SEQS_PER_CHUNK = 99


def _get_fasta_chunks(fasta_path: Path, max_seqs: int) -> list[str]:
    """Parse a FASTA file and return chunks of up to max_seqs sequences."""
    chunks: list[str] = []
    current_chunk = ""
    seq_count = 0

    for line in fasta_path.read_text(encoding="utf-8").splitlines(keepends=True):
        if line.startswith(">"):
            if seq_count == max_seqs:
                chunks.append(current_chunk)
                current_chunk = ""
                seq_count = 0
            seq_count += 1
        current_chunk += line

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def _submit_job(email: str, sequence_data: str) -> str | None:
    """Submit sequence data to the InterProScan REST API. Returns job ID or None."""
    response = requests.post(
        f"{BASE_URL}/run",
        data={
            "email": email,
            "stype": "p",
            "sequence": sequence_data,
            "goterms": "false",
            "pathways": "false",
        },
    )
    if response.status_code == 200:
        return response.text.strip()
    return None


def _wait_for_job(job_id: str) -> bool:
    """Poll until the job finishes. Returns True if FINISHED, False on error."""
    status_url = f"{BASE_URL}/status/{job_id}"
    while True:
        status = requests.get(status_url).text.strip()
        if status == "FINISHED":
            return True
        if status in ("ERROR", "FAILURE", "NOT_FOUND"):
            return False
        time.sleep(15)


def _download_result(job_id: str, output_path: Path) -> bool:
    """Download TSV result for a finished job. Returns True on success."""
    response = requests.get(f"{BASE_URL}/result/{job_id}/tsv")
    if response.status_code == 200:
        output_path.write_text(response.text, encoding="utf-8")
        return True
    return False


@asset(
    required_resource_keys={"local_resource"},
    description=(
        "Submits new protein FASTA files to the InterProScan 5 REST API in chunks "
        "of up to 99 sequences. Downloads each chunk result as a TSV file into the "
        "interpro chunks directory. Returns paths to all newly downloaded chunk TSVs."
    ),
    compute_kind="Python",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def run_interpro_scan(context, create_fasta_p) -> list[str]:
    email = os.getenv("EMAIL")
    if not email:
        raise ValueError("EMAIL environment variable is not set")

    chunks_dir = Path(
        context.resources.local_resource.get_paths()["INTERPRO_CHUNKS_DIR"]
    )
    chunks_dir.mkdir(parents=True, exist_ok=True)

    new_fasta_files: list[str] = create_fasta_p.new
    context.log.info(f"New FASTA files to scan: {len(new_fasta_files)}")

    all_new_chunk_paths: list[str] = []

    for fasta_file in new_fasta_files:
        fasta_path = Path(fasta_file)
        if not fasta_path.exists():
            context.log.warning(f"FASTA file not found, skipping: {fasta_file}")
            continue

        context.log.info(f"Processing {fasta_path.name}")
        chunks = _get_fasta_chunks(fasta_path, MAX_SEQS_PER_CHUNK)
        context.log.info(f"  Split into {len(chunks)} chunk(s)")

        for i, chunk_data in enumerate(chunks, 1):
            context.log.info(
                f"  Submitting chunk {i}/{len(chunks)} for {fasta_path.name}"
            )

            job_id = _submit_job(email, chunk_data)
            if not job_id:
                context.log.warning(f"  Chunk {i} submission failed, skipping")
                continue

            context.log.info(f"  Job submitted: {job_id}. Polling for completion...")

            if not _wait_for_job(job_id):
                context.log.warning(
                    f"  Job {job_id} did not finish successfully, skipping"
                )
                continue

            chunk_tsv = chunks_dir / f"{fasta_path.stem}_chunk_{i}_{job_id}.tsv"
            context.log.info(f"  Downloading result to {chunk_tsv.name}")

            if _download_result(job_id, chunk_tsv):
                all_new_chunk_paths.append(str(chunk_tsv))
                context.log.info(f"  Chunk {i} downloaded successfully")
            else:
                context.log.warning(f"  Failed to download result for job {job_id}")

            time.sleep(3)

    context.log.info(
        f"InterProScan complete. {len(all_new_chunk_paths)} new chunk TSV(s) downloaded."
    )

    context.add_output_metadata(
        metadata={
            "chunks_dir": str(chunks_dir),
            "num_new_fasta_files": len(new_fasta_files),
            "num_new_chunks": len(all_new_chunk_paths),
            "new_chunk_files": all_new_chunk_paths,
        }
    )

    return all_new_chunk_paths
