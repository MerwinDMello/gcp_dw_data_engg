CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_reflocationmeditechexpanse
(
  locationkey INT64 NOT NULL,
  regionkey INT64,
  locationname STRING NOT NULL,
  locationtype STRING,
  siteid STRING,
  coid STRING,
  deleteflag INT64,
  sourceprimarykeyvalue STRING,
  sourcearecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (locationkey) NOT ENFORCED
)
;