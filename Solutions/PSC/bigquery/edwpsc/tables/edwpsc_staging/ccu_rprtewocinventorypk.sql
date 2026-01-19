CREATE TABLE IF NOT EXISTS edwpsc_staging.ccu_rprtewocinventorypk
(
  snapshotdate DATE,
  sourcesystem STRING,
  coid STRING,
  owner STRING,
  inventorytype STRING,
  encounterlock STRING,
  poskey STRING,
  encounterkey INT64,
  businessdayssinceservicedate INT64,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;