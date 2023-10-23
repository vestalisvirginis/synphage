from dagster import (
    AssetSelection,
    define_asset_job,
    ScheduleDefinition,
    op,
    job,
    DynamicOutput,
    DynamicOut,
    Nothing,
    In,
)

import os
import duckdb
import polars as pl


# from .sensors import genbank_file_update_sensor

blasting_job = define_asset_job(
    name="blasting_job",
    selection=AssetSelection.groups("Status")
    | (
        AssetSelection.groups("Blaster")
        & AssetSelection.keys("new_fasta_files")
        .required_multi_asset_neighbors()
        .downstream()
    ),
)


# Job parsing blastn files

@op(out=DynamicOut())
def load():
    for file in os.listdir("data_folder/experimenting/gene_identity/blastn"): #get_blastn: #os.listdir("blastn"):
        yield DynamicOutput(file, mapping_key=file)


@op
def parse(file: str):
    query = """
    with blast as (select unnest(BlastOutput2) as item from read_json_auto('{}')),
    search as (
    SELECT
        item->>'$.report.program' as program,
        item->>'$.report.version' as version,
        item->>'$.report.reference' as reference,
        item->>'$.report.search_target' as search_target,
        item->>'$.report.params' as params,
        item->>'$.report.results.search.query_id' as query_id,
        item->>'$.report.results.search.query_title' as query_title,
        item->>'$.report.results.search.query_len' as query_len,
        item->>'$.report.results.search.hits' as hits,
        item->>'$.report.results.search.hits'->0->'$.num' as number_of_hits,
        item->>'$.report.results.search.hits'->0->'$.description'->0->>'$.title' as title,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.num' as num,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.bit_score' as bit_score,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.score' as score,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.evalue' as evalue,
        cast(item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.identity' as float) as identity,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.query_from' as query_from,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.query_to' as query_to,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.query_strand' as query_strand,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.hit_from' as hit_from,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.hit_to' as hit_to,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.hit_strand' as hit_strand,
        cast(item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.align_len' as float) as align_len,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.gaps' as gaps,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.qseq' as qseq,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.hseq' as hseq,
        item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.midline' as midline,
        regexp_extract(query_title, '^\w+', 0) as query_genome_name,
        regexp_extract(query_title, '\w+\.\d', 0) as query_genome_id,
        regexp_extract(query_title, '\| (\w+) \|', 1) as query_gene,
        regexp_extract(query_title, ' (\w+) \| \[', 1) as query_locus_tag,
        regexp_extract(query_title, '(\[\d+\:\d+\])', 0) as query_start_end,
        regexp_extract(query_title, '(\((\+|\-)\))', 0) as query_gene_strand,
        regexp_extract(title, '^\w+', 0) as source_genome_name,
        regexp_extract(title, '\w+\.\d', 0) as source_genome_id,
        regexp_extract(title, '\| (\w+) \|', 1) as source_gene,
        regexp_extract(title, ' (\w+) \| \[', 1) as source_locus_tag,
        regexp_extract(title, '(\[\d+\:\d+\])', 0) as source_start_end,
        regexp_extract(title, '(\((\+|\-)\))', 0) as source_gene_strand,
        round((identity/align_len)*100,3) as percentage_of_identity
    FROM
        blast
    where
        json_array_length(hits) > 0
    )
    select
    * exclude(params, hits)
    from search
    """

    conn = duckdb.connect(":memory:")
    conn.query(query.format("data_folder/experimenting/gene_identity/blastn/"+file)).pl().write_parquet(f"data_folder/experimenting/tables/{file}.parquet")
    
    return "OK"


@op(ins={"file": In(Nothing)})
def append():
    pl.read_parquet("data_folder/experimenting/tables/*.parquet").write_parquet("genes.parquet")


@job
def transform():
    files = load()
    results = files.map(parse)
    append(results.collect())


# asset_job_sensor = genbank_file_update_sensor(
#     define_asset_job(
#         name="load_job",
#         selection=AssetSelection.groups("Status")
#         | (
#             AssetSelection.groups("Blaster")
#             & AssetSelection.keys("process_asset").downstream()
#         )
#         | AssetSelection.keys("extract_locus_tag_gene"),
#         tags={"dagster/priority": "0"},
#     )
# )


# Job triggering the json files parsing
parsing_job = define_asset_job(
    name="parsing_job",
    selection=AssetSelection.keys("parse_blastn").required_multi_asset_neighbors(),
)

parsing_schedule = ScheduleDefinition(
    job=parsing_job,
    cron_schedule="*/1 * * * *",  # every minute
    tags={"dagster/priority": "1"},
)


# Job triggering the update of the last tables based on Locus_and_gene and blastn dataframes
uniq_job = define_asset_job(
    name="uniq_job",
    selection=AssetSelection.keys("gene_presence_table"),
)

uniq_schedule = ScheduleDefinition(
    job=uniq_job,
    cron_schedule="*/5 * * * *",  # every hour minute???
    tags={"dagster/priority": "2"},
)
