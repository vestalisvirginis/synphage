from dagster import asset, Field, AssetObservation

import os

from Bio import Entrez
from toolz import first
from collections import namedtuple
from typing import List
from pathlib import Path


NucleotideRecord = namedtuple("NucleotideRecord", "dbname,menu,count,status")


def _get_ncbi_count_result(result, dbname) -> NucleotideRecord:
    origin = result["eGQueryResult"]
    return NucleotideRecord(*first(filter(lambda x: x["DbName"] == dbname, origin)).values())


ncbi_query_config = {
        "database": Field(str, description="Database identifier", default_value="nuccore"),
        "keyword": Field(str, description="Search criteria for the ncbi query", default_value="Spbetavirus")
        }

@asset(
        required_resource_keys={"ncbi_connection"},
        config_schema=ncbi_query_config,
        description="Getting the number of records matching the keyword(s) in the specified database",
        compute_kind="ncbi",
        metadata={"owner" : "Virginie Grosboillot"},
)
def accession_count(context) -> int:
    query = context.resources.ncbi_connection.conn.egquery(term = context.op_config["keyword"])
    result = context.resources.ncbi_connection.conn.read(query)
    nucleotide = _get_ncbi_count_result(result, context.op_config["database"])
    num_rows= int(nucleotide.count)
    context.log_event(
        AssetObservation(asset_key="accession_count", metadata={"num_rows": num_rows})
       )
    return num_rows


ncbi_query_config_search = {
    "use_history": Field(str, description="Yes/No value for history", default_value="y"),
    "idtype": Field(str, description="Options for acc", default_value="acc")
}

@asset(
    required_resource_keys={"ncbi_connection"},
    config_schema={**ncbi_query_config, **ncbi_query_config_search},
    description="Getting all accession Ids corresponding to keyword(s)",
    compute_kind="ncbi",
    metadata={"owner" : "Virginie Grosboillot"},
)
def accession_ids(context, accession_count):
    search = context.resources.ncbi_connection.conn.esearch(
        db = context.op_config["database"],
        term = context.op_config["keyword"], 
        retmax = accession_count, 
        usehistory = context.op_config["use_history"], 
        idtype = context.op_config["idtype"]
    )
    result = context.resources.ncbi_connection.conn.read(search)
    context.log_event(
        AssetObservation(asset_key="accession_ids", metadata={"num_rows": len(result["IdList"])})
        )
    search.close()
    return result


download_folder_config = {
    "output_directory" : Field(str, description="Path to folder", default_value="/usr/src/data_folder/genome_download")
}

@asset(
    config_schema = download_folder_config,
    compute_kind = "python"
)
def downloaded_genomes(context) -> List[str]:
    return list(map(lambda x: Path(x).stem, os.listdir(context.op_config["output_directory"])))


ncbi_query_config_fetch = {
    "database": Field(str, description="Database identifier", default_value="nuccore"),
    "rettype": Field(str, description="File format", default_value="gb"),
    #"batch": Field(int, description="Options for acc", default_value=10)
}

@asset(
    required_resource_keys={"ncbi_connection"},
    config_schema={**download_folder_config, **ncbi_query_config_fetch},
    description="Download records one by one from the ncbi database",
    compute_kind="ncbi",
    metadata={"owner" : "Virginie Grosboillot"},
)
def fetch_genome(context, accession_ids, downloaded_genomes) -> List[str]:
    A = set(accession_ids["IdList"])
    B = set(downloaded_genomes)
    C = A.difference(B)
    context.log.info(f"Number of NOT Downloaded: {len(C)}")
    #context.log.info(accession_ids["IdList"])
    for entry in list(C):
        r = context.resources.ncbi_connection.conn.efetch(
            db = context.op_config["database"], 
            id = entry, 
            rettype = context.op_config["rettype"], 
            retmax = 1, 
            webenv = accession_ids["WebEnv"], 
            query_key = accession_ids["QueryKey"]
        )
        path = context.op_config["output_directory"]
        file_name = f"{path}/{entry}.gb"
        with open(file_name, "w") as writer:
            writer.write(r.read())
            
    return list(map(lambda x: f"{path}/{x}", accession_ids["IdList"]))