CREATE TABLE IF NOT EXISTS edwpsc.ecw_refrh1201billclaimstatus
(
  rh1201billclaimstatuskey STRING NOT NULL,
  rh1201billclaimstatusname STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  PRIMARY KEY (rh1201billclaimstatuskey) NOT ENFORCED
)
;