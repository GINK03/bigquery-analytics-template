#standardSQL
CREATE TEMPORARY FUNCTION greeting(a ARRAY<STRING>)
RETURNS STRING
LANGUAGE js AS """
  return a.join(",");
  """;
select 
  greeting(ARRAY_AGG(SchoolName))
  ,city
from
  test.test
group by 
  city
 ;
