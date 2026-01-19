CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refsrtproductivityresultcode
(
  productivityresultcodekey INT64 NOT NULL,
  coid INT64,
  controlnumber INT64,
  invoicename STRING,
  resultcode STRING,
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