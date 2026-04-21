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

foldseek_image = (
    modal.Image.debian_slim()
    .apt_install("wget", "tar")
    .run_commands(
        "mkdir -p /content",
        "cd /content && wget https://mmseqs.com/foldseek/foldseek-linux-gpu.tar.gz",
        "cd /content && tar -xvzf foldseek-linux-gpu.tar.gz",
        "mkdir -p /content/prostt5_weights /content/tmp",
        "/content/foldseek/bin/foldseek databases ProstT5 /content/prostt5_weights /content/tmp",
    )
)


@app.function(image=foldseek_image, gpu="T4", timeout=3600, serialized=True)
def run_foldseek_cluster_remote(fasta_content: str) -> bytes:
    with tempfile.TemporaryDirectory() as work_dir:
        fasta_path = os.path.join(work_dir, "all_proteins.fasta")
        results_dir = os.path.join(work_dir, "foldseek_results")
        results_prefix = os.path.join(results_dir, "results")
        tmp_dir = os.path.join(work_dir, "tmp")
        os.makedirs(results_dir, exist_ok=True)
        os.makedirs(tmp_dir, exist_ok=True)

        with open(fasta_path, "w") as f:
            f.write(fasta_content)

        cmd = [
            "/content/foldseek/bin/foldseek",
            "easy-cluster",
            fasta_path,
            results_prefix,
            tmp_dir,
            "-e",
            "0.001",
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
            raise RuntimeError(f"Foldseek failed with return code {process.returncode}")

        # ZIP the entire results directory and return as bytes
        zip_path = os.path.join(work_dir, "foldseek_results")
        shutil.make_archive(zip_path, "zip", results_dir)

        with open(f"{zip_path}.zip", "rb") as f:
            return f.read()
