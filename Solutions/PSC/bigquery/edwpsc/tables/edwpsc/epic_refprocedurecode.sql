CREATE TABLE IF NOT EXISTS edwpsc.epic_refprocedurecode
(
  procedurecodekey INT64 NOT NULL,
  procedurecode STRING,
  procedurecodename STRING,
  procedurecodedescription STRING,
  procedurecodeshortname STRING,
  procedurecodeactive INT64,
  procid STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (procedurecodekey) NOT ENFORCED
)
;