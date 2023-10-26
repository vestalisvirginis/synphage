with blastn as (select COLUMNS('^(query|source)_.*$'), query_genome_name as name,query_locus_tag as locus_tag from read_parquet('{}')),
     locus as (select * from read_parquet('{}'))
select 
    A.*, B.gene
FROM 
    locus B
LEFT JOIN
    blastn A using('name', 'locus_tag')