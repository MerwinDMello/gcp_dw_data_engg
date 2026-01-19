CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refnbtspecialtycategory
(
  nbtspecialtycategoryidkey INT64 NOT NULL,
  nbtspecialtycategoryid INT64,
  nbtspecialtycategorydesc STRING,
  active BOOL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (nbtspecialtycategoryidkey) NOT ENFORCED
)
;