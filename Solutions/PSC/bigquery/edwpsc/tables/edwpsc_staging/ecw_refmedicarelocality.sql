CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refmedicarelocality
(
  medicarelocalitykey INT64 NOT NULL,
  state STRING,
  zipcode STRING,
  carrier STRING,
  locality STRING,
  ruralind STRING,
  labcblocality STRING,
  ruralind2 STRING,
  plus4flag INT64,
  partbdrugindicator STRING,
  yearqtr INT64,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  medicarelocality STRING
)
;