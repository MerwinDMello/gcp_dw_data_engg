CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refencounteractionlogtype
(
  encounteractionlogtypekey INT64 NOT NULL,
  encounteractionlogtypecode INT64 NOT NULL,
  encounteractionlogtypename STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;