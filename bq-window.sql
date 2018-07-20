#standardSQL
CREATE TEMPORARY FUNCTION greeting(a ARRAY<STRING>)
RETURNS STRING
LANGUAGE js AS """
  return "Hello, !" + a.join(",");
  """;

CREATE TEMPORARY FUNCTION norm(xs ARRAY<STRING>, rank INT64)
RETURNS FLOAT64
LANGUAGE js AS """
  const xs2 = xs.map( x => x.replace("%", "") ).map( x => parseFloat(x) )
  const max = Math.max.apply(null, xs2)
  const xs3 = xs2.map( x => x/max ).map( x => x.toString() )
  return xs3[rank-1];
  """;
select 
  SchoolName
  ,norm( 
    ARRAY_AGG(PercentWhite) over(partition by city order by PercentWhite desc) ,
    Rank() over(partition by city order by PercentWhite desc) 
  )
  ,city
  , PercentWhite
 from
  test.test
 ;

-- ,greeting( 
--   ARRAY_AGG(SchoolName) over(partition by city order by PercentWhite desc) 
-- )-- as monthly_rank 
