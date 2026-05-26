import modal
import os
import shutil
import subprocess
import tempfile

# include_source=False stops Modal from auto-mounting the local Python source
# tree into the container. By default Modal uploads the entire package it finds
# on sys.path (which includes synphage/), causing the container to try to
# import synphage/__init__.py — and fail because dagster is not installed there.
# The function body is entirely self-contained and needs no local source code.
app = modal.App("foldseek-clustering", include_source=False)

# Persistent volume used to hand the (potentially large) input FASTA and the
# results ZIP between the local Dagster process and the remote GPU worker, rather
# than passing the whole protein corpus as a single in-memory function argument
# and returning the results as one big bytes blob.
foldseek_volume = modal.Volume.from_name(
    "foldseek-clustering-data", create_if_missing=True
)
VOLUME_MOUNT = "/data"

# Pinned Foldseek release for reproducible image builds. The previous build
# pulled the rolling "latest" tarball from mmseqs.com, so an upstream update
# could silently change clustering results or break the build. Pin to a tagged
# GitHub release instead; bump FOLDSEEK_VERSION (and FOLDSEEK_SHA256) together.
# Releases: https://github.com/steineggerlab/foldseek/releases
FOLDSEEK_VERSION = "10-941cd33"
FOLDSEEK_URL = (
    "https://github.com/steineggerlab/foldseek/releases/download/"
    f"{FOLDSEEK_VERSION}/foldseek-linux-gpu.tar.gz"
)
# Expected SHA256 of the tarball above. Leave empty to skip verification; the
# build always prints the observed digest (see the sha256sum step below) so it
# can be captured and pinned here later.
FOLDSEEK_SHA256 = ""

# Modal function resources. These are bound at function-definition (import) time
# and so cannot be overridden per-run; they are named here to be discoverable and
# editable in one place rather than buried as decorator literals.
GPU_TYPE = "T4"
FUNCTION_TIMEOUT = 3600
# Retry the remote job on transient GPU/network/container failures.
FUNCTION_RETRIES = 2

# Default Foldseek easy-cluster e-value threshold. The asset can override this
# per run via FoldseekClusterConfig (passed through to the remote function).
DEFAULT_CLUSTER_EVALUE = 0.001

_verify_cmd = (
    f'echo "{FOLDSEEK_SHA256}  foldseek-linux-gpu.tar.gz" | sha256sum -c -'
    if FOLDSEEK_SHA256
    else "echo 'FOLDSEEK_SHA256 not set; skipping checksum verification'"
)

foldseek_image = (
    modal.Image.debian_slim()
    .apt_install("wget", "tar")
    .run_commands(
        "mkdir -p /content",
        f"cd /content && wget -q {FOLDSEEK_URL}",
        "cd /content && sha256sum foldseek-linux-gpu.tar.gz",
        f"cd /content && {_verify_cmd}",
        "cd /content && tar -xzf foldseek-linux-gpu.tar.gz",
        "mkdir -p /content/prostt5_weights /content/tmp",
        "/content/foldseek/bin/foldseek databases ProstT5 /content/prostt5_weights /content/tmp",
    )
)


@app.function(
    image=foldseek_image,
    gpu=GPU_TYPE,
    timeout=FUNCTION_TIMEOUT,
    retries=FUNCTION_RETRIES,
    serialized=True,
    volumes={VOLUME_MOUNT: foldseek_volume},
)
def run_foldseek_cluster_remote(
    input_fasta_name: str,
    output_zip_name: str,
    evalue: float = DEFAULT_CLUSTER_EVALUE,
) -> str:
    """Cluster proteins with Foldseek.

    Reads the input FASTA from the shared volume (``input_fasta_name``), runs
    ``foldseek easy-cluster`` on GPU, and writes the zipped results back to the
    volume as ``output_zip_name``. Returns the output name so the caller can
    download it from the volume.
    """
    # Make sure we see the FASTA the client just uploaded.
    foldseek_volume.reload()
    input_fasta_path = os.path.join(VOLUME_MOUNT, input_fasta_name)

    with tempfile.TemporaryDirectory() as work_dir:
        results_dir = os.path.join(work_dir, "foldseek_results")
        results_prefix = os.path.join(results_dir, "results")
        tmp_dir = os.path.join(work_dir, "tmp")
        os.makedirs(results_dir, exist_ok=True)
        os.makedirs(tmp_dir, exist_ok=True)

        cmd = [
            "/content/foldseek/bin/foldseek",
            "easy-cluster",
            input_fasta_path,
            results_prefix,
            tmp_dir,
            "-e",
            str(evalue),
            "--prostt5-model",
            "/content/prostt5_weights",
            "--gpu",
            "1",
        ]

        print(f"Running command: {' '.join(cmd)}")
        process = subprocess.run(cmd, capture_output=True, text=True)

        if process.returncode != 0:
            print(f"STDOUT: {process.stdout}")
            print(f"STDERR: {process.stderr}")
            # Include a tail of stderr in the exception so the failure cause is
            # visible in the Dagster logs, not only in the Modal container log.
            stderr_tail = (process.stderr or "").strip()[-1000:]
            raise RuntimeError(
                f"Foldseek failed with return code {process.returncode}. "
                f"stderr tail: {stderr_tail}"
            )

        # ZIP the results directory and stage it on the volume for the client.
        zip_stem = os.path.join(work_dir, "foldseek_results")
        shutil.make_archive(zip_stem, "zip", results_dir)
        shutil.copyfile(f"{zip_stem}.zip", os.path.join(VOLUME_MOUNT, output_zip_name))
        foldseek_volume.commit()

    return output_zip_name
