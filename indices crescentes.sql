/*indices crescentes*/
CREATE TABLE ifood_trusted.indices
WITH (
  format='PARQUET'
) AS
select number from (
select sequence (1,100) seq
  )
  cross join unnest(seq) as t(number)