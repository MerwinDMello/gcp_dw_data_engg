CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_reffinancialpayorgroupsize
(
  ranknumber INT64,
  financialpayorgroupname STRING,
  financialpayorgroupsize STRING,
  charges13month NUMERIC(33, 4),
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;