# import os
# from dagster import op, job, DynamicOutput, DynamicOut, Definitions, UPathIOManager, asset, Out, OutputContext, InputContext, Nothing, In
# from functools import reduce
# from toolz import compose
# from typing import List, Union
# from upath import UPath
# import polars as pl
# import duckdb
# from string import Template

# class ParquetIOManager(UPathIOManager):
#     """IO Manager to persist assets as parquet files"""

#     extension: str = ".parquet"

#     def dump_to_path(
#         self,
#         context: OutputContext,
#         obj: pl.DataFrame,
#         path: UPath,
#     ):
#         """Persist Pyspark DataFrame"""
#         obj.write_parquet(str(path))

#     def load_from_path(self, context: InputContext, path: UPath) -> pl.DataFrame:
#         """Load parquet file as PySpark dataframe"""
#         return pl.read_parquet(path)


# @op(out=DynamicOut())
# def load():
#     for file in os.listdir("blastn"):
#         yield DynamicOutput(file, mapping_key=file)


# @op
# def parse(file: str):

#     query = """
#     with blast as (select unnest(BlastOutput2) as item from read_json_auto('blastn/{}')),
#     search as (
#     SELECT
#         item->>'$.report.program' as program,
#         item->>'$.report.version' as version,
#         item->>'$.report.reference' as reference,
#         item->>'$.report.search_target' as search_target,
#         item->>'$.report.params' as params,
#         item->>'$.report.results.search.query_id' as query_id,
#         item->>'$.report.results.search.query_title' as query_title,
#         item->>'$.report.results.search.query_len' as query_len,
#         item->>'$.report.results.search.hits' as hits,
#         item->>'$.report.results.search.hits'->0->'$.num' as number_of_hits,
#         item->>'$.report.results.search.hits'->0->'$.description'->0->>'$.title' as title,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.num' as num,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.bit_score' as bit_score,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.score' as score,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.evalue' as evalue,
#         cast(item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.identity' as float) as identity,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.query_from' as query_from,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.query_to' as query_to,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.query_strand' as query_strand,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.hit_from' as hit_from,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.hit_to' as hit_to,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.hit_strand' as hit_strand,
#         cast(item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.align_len' as float) as align_len,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.gaps' as gaps,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.qseq' as qseq,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.hseq' as hseq,
#         item->>'$.report.results.search.hits'->0->'$.hsps'->0->>'$.midline' as midline,
#         regexp_extract(query_title, '^\w+', 0) as query_genome_name,
#         regexp_extract(query_title, '\w+\.\d', 0) as query_genome_id,
#         regexp_extract(query_title, '\| (\w+) \|', 1) as query_gene,
#         regexp_extract(query_title, ' (\w+) \| \[', 1) as query_locus_tag,
#         regexp_extract(query_title, '(\[\d+\:\d+\])', 0) as query_start_end,
#         regexp_extract(query_title, '(\((\+|\-)\))', 0) as query_gene_strand,
#         regexp_extract(title, '^\w+', 0) as source_genome_name,
#         regexp_extract(title, '\w+\.\d', 0) as source_genome_id,
#         regexp_extract(title, '\| (\w+) \|', 1) as source_gene,
#         regexp_extract(title, ' (\w+) \| \[', 1) as source_locus_tag,
#         regexp_extract(title, '(\[\d+\:\d+\])', 0) as source_start_end,
#         regexp_extract(title, '(\((\+|\-)\))', 0) as source_gene_strand,
#         round((identity/align_len)*100,3) as percentage_of_identity
#     FROM
#         blast
#     where
#         json_array_length(hits) > 0
#     )
#     select
#     * exclude(params, hits)
#     from search
#     """

#     conn = duckdb.connect(":memory:")
#     conn.query(query.format(file)).pl().write_parquet(f"parquets/{file}.parquet")
#     return "OK"


# @op(ins={"file" : In(Nothing)})
# def append():
#     pl.read_parquet("parquets/*.parquet").write_parquet("genes.parquet")


# # @job
# # def transform():
# #     files = load()
# #     results = files.map(parse)
# #     append(results.collect())


# # defs = Definitions(
# #     jobs=[transform]
# #     )
