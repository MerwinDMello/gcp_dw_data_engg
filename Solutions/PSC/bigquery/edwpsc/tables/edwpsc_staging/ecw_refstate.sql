CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refstate
(
  statekey STRING NOT NULL,
  statename STRING NOT NULL,
  statecapitalcity STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (statekey) NOT ENFORCED
)
;