#standardSQL
CREATE TEMPORARY FUNCTION prev(xs ARRAY<STRING>, index INT64)
RETURNS FLOAT64
LANGUAGE js AS """
  const xs1 = xs.map( function(x) {
    if( x == null ) 
      return "0"; 
    else 
      return x;
  });
  const xs2 = xs1.map( x => x.replace(",", "") ).map( x => x.replace("$", "") ).map( x => parseFloat(x) );
  const ret = xs2[index-1-1] - xs2[index-1];
  if( ret == null || isNaN(ret)) 
    return 0.0;
  else
    return ret
  """;
select 
  SchoolName
  ,prev( 
    ARRAY_AGG(SchoolIncomeEstimate) over(partition by city order by SchoolIncomeEstimate desc) ,
    row_number() over(partition by city order by SchoolIncomeEstimate desc) 
  )
  ,city
  ,SchoolIncomeEstimate
from
  test.test;
