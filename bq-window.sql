#standardSQL
CREATE TEMPORARY FUNCTION greeting(a ARRAY<STRING>)
RETURNS STRING
LANGUAGE js AS """
  return "Hello, !" + a.join(",");
  """;
select 
  SchoolName
  ,city
  ,greeting( 
    ARRAY_AGG(SchoolName) over(partition by city order by PercentWhite desc) 
  )-- as monthly_rank 
 from
  test.test
 ;
