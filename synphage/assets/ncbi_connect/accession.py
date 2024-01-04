from dagster import asset, Field, AssetObservation, EnvVar

import os
import tempfile

from toolz import first
from collections import namedtuple
from typing import List
from pathlib import Path
from datetime import datetime


TEMP_DIR = tempfile.gettempdir()

NucleotideRecord = namedtuple("NucleotideRecord", "dbname,menu,count,status")


def _get_ncbi_count_result(result, dbname) -> NucleotideRecord:
    _origin = result["eGQueryResult"]
    return NucleotideRecord(
        *first(filter(lambda x: x["DbName"] == dbname, _origin)).values()
    )


ncbi_query_config = {
    "database": Field(str, description="Database identifier", default_value="nuccore"),
    "use_history": Field(
        str, description="Yes/No value for history", default_value="y"
    ),
    "idtype": Field(str, description="Options for acc", default_value="acc"),
    "rettype": Field(str, description="File format", default_value="gb"),  # gbwithparts
}


@asset(
    required_resource_keys={"ncbi_connection"},
    config_schema=ncbi_query_config,
    description="Getting the number of records matching the keyword(s) in the specified database",
    compute_kind="NCBI",
    io_manager_key="io_manager",
    metadata={"owner": "Virginie Grosboillot"},
)
def accession_count(context) -> int:
    # Search key - default: Myoalterovirus (2 entries in NCBI database Jan 2024)
    keyword = os.getenv(EnvVar("KEYWORD"), "Myoalterovirus")
    context.log.info(f"Search key(s): {keyword}")
    # Query
    _query = context.resources.ncbi_connection.conn.egquery(term=keyword)
    _result = context.resources.ncbi_connection.conn.read(_query)
    _query.close()
    # Extract number of record for keyword
    _nucleotide = _get_ncbi_count_result(_result, context.op_config["database"])
    _num_rows = int(_nucleotide.count)
    context.log_event(
        AssetObservation(asset_key="accession_count", metadata={"num_rows": _num_rows})
    )

    # Asset user metadata
    _time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"End of query: {_time.isoformat()} (UTC).",
            "search_word(s)": keyword,
            "num_hits": _num_rows,
        }
    )

    return _num_rows


@asset(
    required_resource_keys={"ncbi_connection"},
    config_schema=ncbi_query_config,
    description="Getting all accession Ids corresponding to keyword(s)",
    compute_kind="NCBI",
    io_manager_key="io_manager",
    metadata={"owner": "Virginie Grosboillot"},
)
def accession_ids(context, accession_count) -> dict:
    # Search key - default: Myoalterovirus (2 entries in NCBI database Jan 2024)
    keyword = os.getenv(EnvVar("KEYWORD"), "Myoalterovirus")
    context.log.info(f"Search key(s): {keyword}")
    # Search
    context.log.info("Start NCBI database search")
    _search = context.resources.ncbi_connection.conn.esearch(
        db=context.op_config["database"],
        term=keyword,
        retmax=accession_count,
        usehistory=context.op_config["use_history"],
        idtype=context.op_config["idtype"],
    )
    context.log.info("The searched is finished")
    _result = context.resources.ncbi_connection.conn.read(_search)
    context.log_event(
        AssetObservation(
            asset_key="accession_ids", metadata={"num_rows": len(_result["IdList"])}
        )
    )
    _search.close()

    # Asset user metadata
    _time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"End of search: {_time.isoformat()} (UTC).",
            "search_word(s)": keyword,
            "num_retrived_ids": len(_result["IdList"]),
            "id_preview": _result["IdList"],
        }
    )

    return _result


download_folder_config = {
    "output_directory": Field(
        str,
        description="Path to folder",
        default_value="download",
    )
}


@asset(
    config_schema=download_folder_config,
    description="In case of multiple search, checked what sequence have already been downloeded",
    compute_kind="python",
    io_manager_key="io_manager",
    metadata={"owner": "Virginie Grosboillot"},
)
def downloaded_genomes(context) -> List[str]:
    # Download directory
    _download_path = str(
        Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR))
        / context.op_config["output_directory"]
    )
    os.makedirs(_download_path, exist_ok=True)
    # List file in download directory
    _downloaded_files = list(map(lambda x: Path(x).stem, os.listdir(_download_path)))

    # Asset user metadata
    _time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"Downloaded files last update: {_time.isoformat()} (UTC).",
            "num_files": len(_downloaded_files),
            "preview": _downloaded_files,
        }
    )

    return _downloaded_files


@asset(
    required_resource_keys={"ncbi_connection"},
    config_schema={**download_folder_config, **ncbi_query_config},
    description="Download records one by one from the ncbi database",
    compute_kind="NCBI",
    io_manager_key="io_manager",
    metadata={"owner": "Virginie Grosboillot"},
)
def fetch_genome(context, accession_ids, downloaded_genomes) -> List[str]:
    # Exclude already downloaded files
    _A = set(accession_ids["IdList"])
    _B = set(downloaded_genomes)
    _C = _A.difference(_B)
    context.log.info(f"Number of files NOT downloaded: {len(_C)}")
    # Path to download
    _download_path = str(
        Path(os.getenv(EnvVar("DATA_DIR"), TEMP_DIR))
        / context.op_config["output_directory"]
    )
    context.log.info(f"Path to download: {_download_path}")
    # Fetch and write files
    for _entry in list(_C):
        _r = context.resources.ncbi_connection.conn.efetch(
            db=context.op_config["database"],
            id=_entry,
            rettype=context.op_config["rettype"],
            retmax=1,
            webenv=accession_ids["WebEnv"],
            query_key=accession_ids["QueryKey"],
        )

        _file_name = str(Path(_download_path) / f"{_entry}.gb")
        with open(_file_name, "w") as _writer:
            _writer.write(_r.read())

    _all_ids = _B.union(_A)
    _genomes = list(map(lambda x: str(Path(_download_path) / f"{x}.gb"), _all_ids))

    # Asset user metadata
    _time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"Dowloaded files latest status: {_time.isoformat()} (UTC).",
            "num_files": len(_genomes),
            "preview": _genomes,
        }
    )

    return _genomes
