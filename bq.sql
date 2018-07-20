#standardSQL
CREATE TEMPORARY FUNCTION greeting(a STRING)
RETURNS STRING
LANGUAGE js AS """
  return "Hello, " + a + "!";
  """;
SELECT greeting(name) as everyone
FROM UNNEST(["Hannah", "Max", "Jakob"]) AS name;
