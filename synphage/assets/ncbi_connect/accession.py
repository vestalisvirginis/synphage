from dagster import asset, Field, AssetObservation, EnvVar

import os

from toolz import first
from collections import namedtuple
from typing import List
from pathlib import Path


NucleotideRecord = namedtuple("NucleotideRecord", "dbname,menu,count,status")


def _get_ncbi_count_result(result, dbname) -> NucleotideRecord:
    _origin = result["eGQueryResult"]
    return NucleotideRecord(
        *first(filter(lambda x: x["DbName"] == dbname, _origin)).values()
    )


ncbi_query_config = {
    "database": Field(str, description="Database identifier", default_value="nuccore"),
    "keyword": Field(
        str,
        description="Search criteria for the ncbi query",
        default_value="Spbetavirus",
    ),
}


@asset(
    required_resource_keys={"ncbi_connection"},
    config_schema=ncbi_query_config,
    description="Getting the number of records matching the keyword(s) in the specified database",
    compute_kind="NCBI",
    metadata={"owner": "Virginie Grosboillot"},
)
def accession_count(context) -> int:
    _query = context.resources.ncbi_connection.conn.egquery(
        term=context.op_config["keyword"]
    )
    _result = context.resources.ncbi_connection.conn.read(_query)
    _nucleotide = _get_ncbi_count_result(_result, context.op_config["database"])
    _num_rows = int(_nucleotide.count)
    context.log_event(
        AssetObservation(asset_key="accession_count", metadata={"num_rows": _num_rows})
    )
    return _num_rows


ncbi_query_config_search = {
    "use_history": Field(
        str, description="Yes/No value for history", default_value="y"
    ),
    "idtype": Field(str, description="Options for acc", default_value="acc"),
}


@asset(
    required_resource_keys={"ncbi_connection"},
    config_schema={**ncbi_query_config, **ncbi_query_config_search},
    description="Getting all accession Ids corresponding to keyword(s)",
    compute_kind="NCBI",
    metadata={"owner": "Virginie Grosboillot"},
)
def accession_ids(context, accession_count):
    _search = context.resources.ncbi_connection.conn.esearch(
        db=context.op_config["database"],
        term=context.op_config["keyword"],
        retmax=accession_count,
        usehistory=context.op_config["use_history"],
        idtype=context.op_config["idtype"],
    )
    _result = context.resources.ncbi_connection.conn.read(_search)
    context.log_event(
        AssetObservation(
            asset_key="accession_ids", metadata={"num_rows": len(_result["IdList"])}
        )
    )
    _search.close()
    return _result


download_folder_config = {
    "output_directory": Field(
        str,
        description="Path to folder",
        default_value="download",
    )
}


@asset(config_schema=download_folder_config, compute_kind="python")
def downloaded_genomes(context) -> List[str]:
    _download_path = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["output_directory"]]
    )
    return list(map(lambda x: Path(x).stem, os.listdir(_download_path)))


ncbi_query_config_fetch = {
    "database": Field(str, description="Database identifier", default_value="nuccore"),
    "rettype": Field(str, description="File format", default_value="gb"),  # gbwithparts
    # "batch": Field(int, description="Options for acc", default_value=10)
}


@asset(
    required_resource_keys={"ncbi_connection"},
    config_schema={**download_folder_config, **ncbi_query_config_fetch},
    description="Download records one by one from the ncbi database",
    compute_kind="NCBI",
    metadata={"owner": "Virginie Grosboillot"},
)
def fetch_genome(context, accession_ids, downloaded_genomes) -> List[str]:
    _A = set(accession_ids["IdList"])
    _B = set(downloaded_genomes)
    _C = _A.difference(_B)
    context.log.info(f"Number of NOT Downloaded: {len(_C)}")
    _path = context.op_config["output_directory"]

    for _entry in list(_C):
        _r = context.resources.ncbi_connection.conn.efetch(
            db=context.op_config["database"],
            id=_entry,
            rettype=context.op_config["rettype"],
            retmax=1,
            webenv=accession_ids["WebEnv"],
            query_key=accession_ids["QueryKey"],
        )

        _file_name = f"{_path}/{_entry}.gb"
        with open(_file_name, "w") as _writer:
            _writer.write(_r.read())

    return list(map(lambda x: f"{_path}/{x}.gb", accession_ids["IdList"]))
