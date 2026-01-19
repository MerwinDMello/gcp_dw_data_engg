CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factencounternotes
(
  encounternoteskey INT64 NOT NULL,
  regionkey INT64,
  encounterkey INT64,
  encounterid INT64,
  encounternotetype STRING,
  encounternote STRING,
  sourceprimarykeyvalue INT64,
  sourcerecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  archivedrecord STRING NOT NULL
)
;