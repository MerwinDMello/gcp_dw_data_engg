CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refutfdays
(
  iplankey INT64,
  coidstate STRING,
  iplanid INT64,
  appealdays INT64,
  initialsubmissiondays INT64,
  appealtype STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  lastverificationdate DATETIME
)
;