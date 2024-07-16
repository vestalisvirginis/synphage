with blastn as (select COLUMNS('^(query|source)_.*$'), query_key as key from read_parquet('{}')),
     locus as (select * from read_parquet('{}'))
select 
    A.*, B.*
FROM 
    locus B
LEFT JOIN
    blastn A using('key')
