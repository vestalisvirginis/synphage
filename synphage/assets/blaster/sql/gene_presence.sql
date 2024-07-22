with blastn as (select * exclude(query_id, query_strand) from (select COLUMNS('^(query|source)_.*$'), percentage_of_identity from read_parquet('{}'))),
      locus as (select * from read_parquet('{}')),
      query_side as (select COLUMNS('(\w*)') AS 'query_\1' from locus),
      source_side as (select COLUMNS('(\w*)') AS 'source_\1' from locus),
      joinedlocus as (select B.*, A.* exclude(query_key) from query_side B left join blastn A using (query_key))
select C.*, D.* exclude(source_key) FROM joinedlocus C LEFT JOIN source_side D using(source_key)