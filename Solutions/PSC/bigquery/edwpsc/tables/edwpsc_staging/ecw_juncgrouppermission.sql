CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_juncgrouppermission
(
  ecwgrouppermissionkey INT64 NOT NULL,
  regionkey INT64,
  userid INT64,
  userkey INT64,
  groupid INT64,
  permission INT64,
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