import os
import shutil
import subprocess
import tempfile
import zipfile

import modal

# include_source=False stops Modal from auto-mounting the local Python source
# tree into the container. By default Modal uploads the entire package it finds
# on sys.path (which includes synphage/), causing the container to try to
# import synphage/__init__.py — and fail because dagster is not installed there.
# The function body is entirely self-contained and needs no local source code.
app = modal.App("phold-protein-annotation", include_source=False)

# Define the environment: Debian + PyTorch (CUDA) + Phold + Foldseek
phold_image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("wget", "tar", "git")
    .pip_install("torch>=2.6.0", "torchvision", "torchaudio")
    .pip_install("phold")
    .run_commands(
        "mkdir -p /content",
        "cd /content && wget https://mmseqs.com/foldseek/foldseek-linux-gpu.tar.gz",
        "cd /content && tar -xvzf foldseek-linux-gpu.tar.gz",
        "ln -s /content/foldseek/bin/foldseek /usr/local/bin/foldseek",
    )
    # Added --foldseek_gpu so the downloaded databases are formatted for GPU
    .run_commands("phold install -t 4 --foldseek_gpu")
)


@app.function(image=phold_image, gpu="T4", timeout=3600, serialized=True)
def run_phold_remote(fasta_content: str) -> bytes:
    with tempfile.TemporaryDirectory() as work_dir:
        fasta_path = os.path.join(work_dir, "input_proteins.fasta")

        # Create two distinct directories
        predict_dir = os.path.join(work_dir, "predict_out")
        compare_dir = os.path.join(work_dir, "compare_out")
        os.makedirs(predict_dir, exist_ok=True)
        os.makedirs(compare_dir, exist_ok=True)

        with open(fasta_path, "w") as f:
            f.write(fasta_content)

        # Step 1: Predict 3Di tokens
        cmd_predict = [
            "phold",
            "proteins-predict",
            "-i",
            fasta_path,
            "-o",
            predict_dir,
            "-t",
            "4",
            "-f",
            "-p",
            "phold",
        ]
        print(f"Running: {' '.join(cmd_predict)}")
        process_predict = subprocess.run(cmd_predict, capture_output=True, text=True)

        if process_predict.returncode != 0:
            print(f"STDOUT: {process_predict.stdout}")
            print(f"STDERR: {process_predict.stderr}")
            raise RuntimeError(
                f"Phold proteins-predict failed (Code: {process_predict.returncode})"
            )

        # Step 2: Compare the predicted 3Di tokens
        cmd_compare = [
            "phold",
            "proteins-compare",
            "-i",
            fasta_path,
            "--predictions_dir",
            predict_dir,
            "-o",
            compare_dir,
            "-t",
            "4",
            "--foldseek_gpu",
            "-f",
            "-p",
            "phold",
        ]
        print(f"Running: {' '.join(cmd_compare)}")
        process_compare = subprocess.run(cmd_compare, capture_output=True, text=True)

        if process_compare.returncode != 0:
            print(f"STDOUT: {process_compare.stdout}")
            print(f"STDERR: {process_compare.stderr}")
            raise RuntimeError(
                f"Phold proteins-compare failed (Code: {process_compare.returncode})"
            )

        # Combine both directories into one final results folder
        final_dir = os.path.join(work_dir, "phold_final_results")
        os.makedirs(final_dir, exist_ok=True)
        shutil.copytree(
            predict_dir, os.path.join(final_dir, "predictions"), dirs_exist_ok=True
        )
        shutil.copytree(
            compare_dir, os.path.join(final_dir, "annotations"), dirs_exist_ok=True
        )

        # Zip the unified results directory
        zip_path = os.path.join(work_dir, "phold_results")
        shutil.make_archive(zip_path, "zip", final_dir)

        # Return the zipped bytes
        with open(f"{zip_path}.zip", "rb") as f:
            return f.read()
