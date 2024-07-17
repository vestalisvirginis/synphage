with blastn as (select COLUMNS('^(query|source)_.*$'), percentage_of_identity from read_parquet('temp/development/data/tables/blastn_summary.parquet')),
     locus as (select *, key as query_key, name as query_name, id as query_id, locus_tag as query_locus_tag, start_sequence as query_start_sequence, end_sequence as query_end_sequence, strand as query_strand_sequence from read_parquet('temp/development/data/tables/processed_genbank_df.parquet')),
     result as(select key as source_key, name as source_name, id as source_id, locus_tag as source_locus_tag, start_sequence as source_start_sequence, end_sequence as source_end_sequence, strand as source_strand_sequence from locus),
     joinedlocus as (select *, from locus B left join blastn A using('query_key'))
select 
    C.*, D.*
FROM 
    joinedlocus D
LEFT JOIN
    result C using('source_key')
