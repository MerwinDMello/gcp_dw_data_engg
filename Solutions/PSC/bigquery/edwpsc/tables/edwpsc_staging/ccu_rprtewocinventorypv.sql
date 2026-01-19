CREATE TABLE IF NOT EXISTS edwpsc_staging.ccu_rprtewocinventorypv
(
  snapshotdate DATE,
  coid STRING,
  practicename STRING,
  encounterkey INT64,
  encounterlock INT64,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;