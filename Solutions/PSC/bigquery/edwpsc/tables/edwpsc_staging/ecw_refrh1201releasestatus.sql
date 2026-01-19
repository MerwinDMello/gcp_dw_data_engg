CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refrh1201releasestatus
(
  rh1201releasestatuskey STRING NOT NULL,
  rh1201releasestatusname STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  PRIMARY KEY (rh1201releasestatuskey) NOT ENFORCED
)
;