CREATE TABLE IF NOT EXISTS edwpsc.ecw_refnbtspecialty
(
  nbtspecialtyidkey INT64 NOT NULL,
  specialtyid INT64,
  specialtycode STRING,
  specialtydescription STRING,
  specialtytypeid INT64,
  specialtyactive BOOL,
  specialtyuuid STRING,
  nbtspecialtycategoryidkey INT64,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (nbtspecialtyidkey) NOT ENFORCED
)
;