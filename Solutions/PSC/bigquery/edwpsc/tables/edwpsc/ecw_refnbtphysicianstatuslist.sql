CREATE TABLE IF NOT EXISTS edwpsc.ecw_refnbtphysicianstatuslist
(
  nbtphysicianstatuskey INT64 NOT NULL,
  physicianstatuskey STRING,
  physicianstatusdesc STRING,
  physicianstatusdetailid STRING,
  physicianstatusdetaildesc STRING,
  statusyear STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (nbtphysicianstatuskey) NOT ENFORCED
)
;