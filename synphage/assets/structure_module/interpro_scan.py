from dagster import asset, AssetCheckResult, AssetCheckSpec, Config, Output

import os
import time
import requests

from pathlib import Path
from synphage.resources.local_resource import OWNER

BASE_URL = "https://www.ebi.ac.uk/Tools/services/rest/iprscan5"
# InterProScan rejects a whole submission if any sequence is shorter than this
# many residues ("Sequence is too short (Length must be at least 3)"). This is a
# fixed API constraint, not a tunable, so it stays a module constant.
MIN_SEQ_LENGTH = 3

# Network safety: (connect, read) timeout for every HTTP call so a stalled EBI
# connection cannot hang the Dagster run indefinitely.
REQUEST_TIMEOUT = (10, 60)


class InterproScanConfig(Config):  # type: ignore[misc]
    """Run-time knobs for the InterProScan submission asset."""

    # The EBI REST API accepts up to 100 sequences per submission; 99 keeps a
    # safe margin.
    max_seqs_per_chunk: int = 99
    # How often to poll a submitted job for completion (seconds).
    poll_interval_seconds: int = 15
    # Overall wall-clock budget for a single job to reach FINISHED before giving
    # up (seconds). Guards against a job stuck in RUNNING/QUEUED forever.
    job_max_wait_seconds: int = 3600
    # Seconds to pause between submissions (polite API usage).
    submission_delay_seconds: int = 3
    # Request GO term / pathway annotations alongside the domain hits.
    request_goterms: bool = False
    request_pathways: bool = False
    # Contact email required by the EBI API. Defaults to the EMAIL env var when
    # left blank, but can be set explicitly via run config for testability.
    email: str = ""


def _parse_fasta_records(fasta_path: Path) -> list[tuple[str, int]]:
    """Parse a FASTA file into (record_text, residue_count) tuples.

    record_text is the full record (header line + sequence lines, including
    newlines). residue_count is the number of non-whitespace sequence
    characters, used to detect empty records.
    """
    records: list[tuple[str, int]] = []
    header: str | None = None
    seq_lines: list[str] = []

    def flush() -> None:
        if header is not None:
            residues = "".join(seq_lines)
            residue_count = len("".join(residues.split()))
            records.append((header + "".join(seq_lines), residue_count))

    for line in fasta_path.read_text(encoding="utf-8").splitlines(keepends=True):
        if line.startswith(">"):
            flush()
            header = line
            seq_lines = []
        elif header is not None:
            seq_lines.append(line)
    flush()

    return records


def _get_fasta_chunks(fasta_path: Path, max_seqs: int) -> tuple[list[str], int]:
    """Return (chunks, skipped_short).

    Records with fewer than MIN_SEQ_LENGTH sequence residues are dropped
    (InterProScan rejects a whole submission if any sequence is too short).
    Remaining records are grouped into chunks of up to max_seqs sequences.
    skipped_short is the number of records that were discarded.
    """
    records = _parse_fasta_records(fasta_path)
    valid = [
        text for text, residue_count in records if residue_count >= MIN_SEQ_LENGTH
    ]
    skipped_short = len(records) - len(valid)

    chunks: list[str] = [
        "".join(valid[i : i + max_seqs]) for i in range(0, len(valid), max_seqs)
    ]

    return chunks, skipped_short


def _submit_job(
    email: str,
    sequence_data: str,
    log,
    goterms: bool = False,
    pathways: bool = False,
) -> str | None:
    """Submit sequence data to the InterProScan REST API. Returns job ID or None."""
    try:
        response = requests.post(
            f"{BASE_URL}/run",
            data={
                "email": email,
                "stype": "p",
                "sequence": sequence_data,
                "goterms": "true" if goterms else "false",
                "pathways": "true" if pathways else "false",
            },
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as exc:
        log.warning(f"    Submission request error: {exc}")
        return None

    if response.status_code == 200:
        return response.text.strip()

    log.warning(
        f"    Submission rejected (HTTP {response.status_code}): "
        f"{response.text.strip()[:1000]}"
    )
    return None


def _wait_for_job(
    job_id: str,
    log,
    poll_interval: int = 15,
    max_wait_seconds: int = 3600,
) -> bool:
    """Poll until the job finishes. Returns True if FINISHED, False on error or
    if the overall max_wait_seconds deadline is exceeded."""
    status_url = f"{BASE_URL}/status/{job_id}"
    deadline = time.monotonic() + max_wait_seconds
    while True:
        try:
            status = requests.get(status_url, timeout=REQUEST_TIMEOUT).text.strip()
        except requests.RequestException as exc:
            log.warning(f"    Status poll error for job {job_id}: {exc}")
            return False

        if status == "FINISHED":
            return True
        if status in ("ERROR", "FAILURE", "NOT_FOUND"):
            log.warning(f"    Job {job_id} ended with status {status}")
            error_detail = _fetch_job_error(job_id)
            if error_detail:
                log.warning(f"    Job {job_id} error detail: {error_detail[:1000]}")
            return False

        if time.monotonic() >= deadline:
            log.warning(
                f"    Job {job_id} did not finish within {max_wait_seconds}s "
                f"(last status: {status}); giving up."
            )
            return False
        time.sleep(poll_interval)


def _fetch_job_error(job_id: str) -> str | None:
    """Best-effort fetch of the API error log for a failed job."""
    try:
        response = requests.get(
            f"{BASE_URL}/result/{job_id}/error", timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException:
        return None
    if response.status_code == 200:
        return response.text.strip()
    return None


def _download_result(job_id: str, output_path: Path, log) -> bool:
    """Download TSV result for a finished job. Returns True on success."""
    try:
        response = requests.get(
            f"{BASE_URL}/result/{job_id}/tsv", timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException as exc:
        log.warning(f"    Result download error for job {job_id}: {exc}")
        return False

    if response.status_code == 200:
        output_path.write_text(response.text, encoding="utf-8")
        return True

    log.warning(
        f"    Result download failed for job {job_id} (HTTP {response.status_code}): "
        f"{response.text.strip()[:1000]}"
    )
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
    check_specs=[
        AssetCheckSpec(
            name="all_chunks_completed",
            asset="run_interpro_scan",
            description=(
                "Every chunk submitted to the InterProScan API must have a downloaded "
                "result TSV. A mismatch indicates API failures during the run."
            ),
        ),
    ],
)
def run_interpro_scan(context, config: InterproScanConfig, create_fasta_p):
    email = config.email or os.getenv("EMAIL")
    if not email:
        raise ValueError(
            "No InterProScan contact email set (config.email or EMAIL env var)"
        )

    chunks_dir = Path(
        context.resources.local_resource.get_paths()["INTERPRO_CHUNKS_DIR"]
    )
    chunks_dir.mkdir(parents=True, exist_ok=True)

    new_fasta_files: list[str] = create_fasta_p.new
    context.log.info(f"New FASTA files to scan: {len(new_fasta_files)}")

    all_chunk_paths: list[str] = []
    expected_chunk_count = 0
    already_completed_count = 0
    newly_downloaded_count = 0
    skipped_short_count = 0
    # fasta filename -> number of chunks that failed (submission, job error, or download)
    failed_chunks_per_file: dict[str, int] = {}

    for fasta_file in new_fasta_files:
        fasta_path = Path(fasta_file)
        if not fasta_path.exists():
            context.log.warning(f"FASTA file not found, skipping: {fasta_file}")
            continue

        context.log.info(f"Processing {fasta_path.name}")
        chunks, skipped_short = _get_fasta_chunks(
            fasta_path, config.max_seqs_per_chunk
        )
        skipped_short_count += skipped_short
        if skipped_short:
            context.log.warning(
                f"  Skipped {skipped_short} record(s) shorter than "
                f"{MIN_SEQ_LENGTH} residues in {fasta_path.name}"
            )
        context.log.info(f"  Split into {len(chunks)} chunk(s)")
        expected_chunk_count += len(chunks)
        file_failures = 0

        for i, chunk_data in enumerate(chunks, 1):
            # Skip chunks that already have a result from a previous run
            existing = list(chunks_dir.glob(f"{fasta_path.stem}_chunk_{i}_*.tsv"))
            if existing:
                context.log.info(
                    f"  Chunk {i} already completed ({existing[0].name}), skipping"
                )
                all_chunk_paths.append(str(existing[0]))
                already_completed_count += 1
                continue

            context.log.info(
                f"  Submitting chunk {i}/{len(chunks)} for {fasta_path.name}"
            )

            job_id = _submit_job(
                email,
                chunk_data,
                context.log,
                goterms=config.request_goterms,
                pathways=config.request_pathways,
            )
            if not job_id:
                context.log.warning(f"  Chunk {i} submission failed, skipping")
                file_failures += 1
                continue

            context.log.info(f"  Job submitted: {job_id}. Polling for completion...")

            if not _wait_for_job(
                job_id,
                context.log,
                poll_interval=config.poll_interval_seconds,
                max_wait_seconds=config.job_max_wait_seconds,
            ):
                context.log.warning(
                    f"  Job {job_id} did not finish successfully, skipping"
                )
                file_failures += 1
                continue

            chunk_tsv = chunks_dir / f"{fasta_path.stem}_chunk_{i}_{job_id}.tsv"
            context.log.info(f"  Downloading result to {chunk_tsv.name}")

            if _download_result(job_id, chunk_tsv, context.log):
                all_chunk_paths.append(str(chunk_tsv))
                newly_downloaded_count += 1
                context.log.info(f"  Chunk {i} downloaded successfully")
            else:
                context.log.warning(f"  Failed to download result for job {job_id}")
                file_failures += 1

            time.sleep(config.submission_delay_seconds)

        if file_failures:
            failed_chunks_per_file[fasta_path.name] = file_failures

    successful_chunk_count = already_completed_count + newly_downloaded_count
    total_failed = expected_chunk_count - successful_chunk_count
    context.log.info(
        f"InterProScan complete. {successful_chunk_count}/{expected_chunk_count} chunks OK "
        f"({already_completed_count} already done, {newly_downloaded_count} newly downloaded, "
        f"{total_failed} failed, {skipped_short_count} short record(s) skipped)."
    )

    yield Output(
        value=all_chunk_paths,
        metadata={
            "chunks_dir": str(chunks_dir),
            "num_new_fasta_files": len(new_fasta_files),
            "expected_chunks": expected_chunk_count,
            "already_completed_chunks": already_completed_count,
            "newly_downloaded_chunks": newly_downloaded_count,
            "failed_chunks": total_failed,
            "skipped_short_records": skipped_short_count,
            "chunk_files": all_chunk_paths,
        },
    )

    yield AssetCheckResult(
        check_name="all_chunks_completed",
        passed=total_failed == 0,
        metadata={
            "expected_chunks": expected_chunk_count,
            "already_completed_chunks": already_completed_count,
            "newly_downloaded_chunks": newly_downloaded_count,
            "failed_chunks": total_failed,
            "skipped_short_records": skipped_short_count,
            "failed_files": failed_chunks_per_file,
        },
    )
