CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refcoidsum300
(
  coid STRING NOT NULL,
  fein STRING,
  hcaps300 STRING,
  conscoid STRING,
  flevel STRING,
  legalname STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;