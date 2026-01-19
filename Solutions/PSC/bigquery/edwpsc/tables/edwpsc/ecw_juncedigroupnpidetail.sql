CREATE TABLE IF NOT EXISTS edwpsc.ecw_juncedigroupnpidetail
(
  ecwedigroupnpidetailkey INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  npiruleid INT64,
  providerid INT64,
  providerkey INT64,
  facilityid INT64 NOT NULL,
  facilitykey INT64,
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