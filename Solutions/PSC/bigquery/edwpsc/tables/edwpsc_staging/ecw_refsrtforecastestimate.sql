CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refsrtforecastestimate
(
  forecastestimatekey INT64 NOT NULL,
  years INT64,
  months STRING,
  pedate DATE,
  itemnumber INT64,
  owners STRING,
  controlnumber INT64,
  valescoind STRING,
  originalitemnumber INT64,
  coid INT64,
  vendor STRING,
  notes STRING,
  deptartment INT64,
  initialcmsrtestimates INT64,
  finalcmsrtestimates INT64,
  sourceaprimarykeyvalue INT64,
  deleteflag INT64 NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME NOT NULL,
  modifiedby STRING,
  modifieddtm DATETIME
)
;