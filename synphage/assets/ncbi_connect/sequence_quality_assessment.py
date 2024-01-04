from dagster import (
    asset,
    EnvVar,
    Field,
)

import os
import shutil

from pathlib import Path
from datetime import datetime
from typing import List


sqc_folder_config = {
    "sqc_download_dir": Field(
        str,
        description="Path to folder containing the downloaded genbank sequences",
        default_value="download",
    ),
    "genbank_dir": Field(
        str,
        description="Path to folder containing the genbank files",
        default_value="genbank",
    ),
    "fasta_dir": Field(
        str,
        description="Path to folder containing the fasta sequence files",
        default_value="gene_identity/fasta",
    ),
}


@asset(
    config_schema={**sqc_folder_config},
    description="Checks for sequence quality and accuracy",
    compute_kind="Biopython",
    metadata={"owner": "Virginie Grosboillot"},
)
def sequence_check(context, fetch_genome) -> List[str]:
    context.log.info(f"Number of genomes in download folder: {len(fetch_genome)}")

    _gb_path = "/".join(
        [os.getenv(EnvVar("DATA_DIR")), context.op_config["genbank_dir"]]
    )
    os.makedirs(_gb_path, exist_ok=True)

    # add check to assess the quality of the query

    for _file in fetch_genome:
        shutil.copy2(
            _file,
            f"{_gb_path}/{Path(_file).stem.replace('.', '_')}.gb",
        )

    _downloaded_files = list(
        map(
            lambda x: Path(x).stem,
            os.listdir(_gb_path),
        )
    )

    _time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"List of downloaded sequences{_time.isoformat()} (UTC).",
            "path": _gb_path,
            "num_files": len(_downloaded_files),
            "preview": _downloaded_files,
        }
    )

    return _downloaded_files
