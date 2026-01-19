CREATE TABLE IF NOT EXISTS edwpsc.ecw_juncprtxidsfacility
(
  ecwprtxidsfacilitykey INT64 NOT NULL,
  regionkey INT64,
  prtxruleid INT64,
  facilityid INT64,
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