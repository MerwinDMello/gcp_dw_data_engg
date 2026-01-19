CREATE TABLE IF NOT EXISTS edwpsc.ccu_rprtewocinventoryecw
(
  snapshotdate DATE,
  coid STRING,
  poskey INT64,
  encounterkey INT64,
  encounterlock INT64,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
PARTITION BY snapshotdate
;