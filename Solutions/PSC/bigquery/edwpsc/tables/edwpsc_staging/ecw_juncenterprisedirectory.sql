CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_juncenterprisedirectory
(
  ecwenterprisedirectorykey INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  orgid INT64 NOT NULL,
  parentid INT64 NOT NULL,
  orgname STRING,
  orgdesc STRING,
  orgtype STRING,
  deleteflag INT64,
  sourceprimarykeyvalue INT64,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;