CREATE TABLE IF NOT EXISTS edwpsc.ecw_factencounteractionlog
(
  encounteractionlogid INT64 NOT NULL,
  encounterkey INT64 NOT NULL,
  encounteractionlogtypekey INT64 NOT NULL,
  encounteractionlognote STRING,
  encounteractionlogdatekey DATE,
  encounteractionlogtime STRING,
  encounteractionlogcreatedbyuserkey INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  sourceprimarykeyvalue INT64 NOT NULL,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  archivedrecord STRING NOT NULL
)
;