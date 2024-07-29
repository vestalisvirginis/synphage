from dagster import asset, AssetObservation, Config, ConfigArgumentWarning
from pydantic import Field

import os
import warnings

from typing import List
from pathlib import Path
from synphage.resources.local_resource import OWNER


warnings.filterwarnings("ignore", category=ConfigArgumentWarning)


class QueryConfig(Config):  # type: ignore[misc] # should be ok in 1.8 version of Dagster
    search_key: str = Field(
        default="Myoalterovirus", description="Keyword(s) for NCBI query"
    )
    database: str = Field(default="nuccore", description="Database identifier")
    use_history: str = Field(default="y", description="Yes/No value for history")
    idtype: str = Field(default="acc", description="Options for acc")
    rettype: str = Field(default="gb", description="File format")  # gbwithparts


@asset(
    description="Set up the configuration for the NCBI search.",
    compute_kind="Config",
    metadata={"owner": OWNER},
)
def setup_query_config(config: QueryConfig) -> QueryConfig:
    """Search parameters"""
    return config


@asset(
    required_resource_keys={"ncbi_connection"},
    description="Getting the number of records matching the keyword(s) in the specified database",
    compute_kind="NCBI",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def accession_count(context, setup_query_config: QueryConfig) -> int:
    # Search key - default: Myoalterovirus (2 entries in NCBI database Jan 2024)
    keyword = setup_query_config.search_key
    db = setup_query_config.database
    context.log.info(f"Search key(s): {keyword}")
    # Query
    _query = context.resources.ncbi_connection.conn.esearch(term=keyword, db=db)
    _result = context.resources.ncbi_connection.conn.read(_query)
    _query.close()
    # Extract number of record for keyword
    _num_rows = int(_result["Count"])
    _ncbi_query = _result["QueryTranslation"]
    context.log_event(
        AssetObservation(asset_key="accession_count", metadata={"num_rows": _num_rows})
    )

    # Asset user metadata
    context.add_output_metadata(
        metadata={
            "num_hits": _num_rows,
            "search_word(s)": keyword,
            "database": db,
            "NCBI_query": _ncbi_query,
        }
    )

    return _num_rows


@asset(
    required_resource_keys={"ncbi_connection"},
    description="Getting all accession Ids corresponding to keyword(s)",
    compute_kind="NCBI",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def accession_ids(context, accession_count, setup_query_config: QueryConfig) -> dict:
    # Search key - default: Myoalterovirus (2 entries in NCBI database Jan 2024)
    keyword = setup_query_config.search_key
    db = setup_query_config.database
    context.log.info(f"Search key(s): {keyword}")
    # Search
    context.log.info("Start NCBI database search")
    _search = context.resources.ncbi_connection.conn.esearch(
        db=db,
        term=keyword,
        retmax=accession_count,
        usehistory=setup_query_config.use_history,
        idtype=setup_query_config.idtype,
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
    context.add_output_metadata(
        metadata={
            "num_retrived_ids": len(_result["IdList"]),
            "id_preview": _result["IdList"],
            "database": db,
            "search_word(s)": keyword,
        }
    )

    return _result


@asset(
    required_resource_keys={"local_resource"},
    description="In case of multiple searches, check what sequences have already been downloaded.",
    compute_kind="python",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def downloaded_genomes(context) -> List[str]:
    # Download directory
    _download_path = context.resources.local_resource.get_paths()["DOWNLOAD_DIR"]
    os.makedirs(_download_path, exist_ok=True)
    # List file in download directory
    _downloaded_files = list(map(lambda x: Path(x).stem, os.listdir(_download_path)))

    # Asset user metadata
    context.add_output_metadata(
        metadata={
            "folder": _download_path,
            "num_files": len(_downloaded_files),
            "preview": _downloaded_files,
        }
    )

    return _downloaded_files


@asset(
    required_resource_keys={"ncbi_connection", "local_resource"},
    description="Download records one by one from the ncbi database",
    compute_kind="NCBI",
    io_manager_key="io_manager",
    metadata={"owner": OWNER},
)
def fetch_genome(
    context, accession_ids, downloaded_genomes, setup_query_config: QueryConfig
) -> List[str]:
    # Exclude already downloaded files
    _A = set(accession_ids["IdList"])
    _B = set(downloaded_genomes)
    _C = _A.difference(_B)
    context.log.info(f"Number of files NOT downloaded: {len(_C)}")
    # Path to download
    _download_path = context.resources.local_resource.get_paths()["DOWNLOAD_DIR"]
    context.log.info(f"Download in process to: {_download_path}")
    # Fetch and write files
    _new_download = []
    count = 0
    for _entry in list(_C):
        count += 1
        context.log.info(f"Downloading {_entry}, ({count/len(_C)})")
        _r = context.resources.ncbi_connection.conn.efetch(
            db=setup_query_config.database,
            id=_entry,
            rettype=setup_query_config.rettype,
            retmax=1,
            webenv=accession_ids["WebEnv"],
            query_key=accession_ids["QueryKey"],
        )

        _file_name = str(Path(_download_path) / f"{_entry}.gb")
        _new_download.append(_file_name)
        with open(_file_name, "w") as _writer:
            _writer.write(_r.read())
        context.log.info("Done")

    context.log.info("Download completed successfully!")
    _all_ids = _B.union(_A)
    _genomes = list(map(lambda x: str(Path(_download_path) / f"{x}.gb"), _all_ids))

    # Asset user metadata
    context.add_output_metadata(
        metadata={
            "downloaded_files": len(_new_download),
            "dowloaded_files_preview": _new_download,
            "total_files": len(_genomes),
            "total_preview": _genomes,
        }
    )

    return _genomes
