CREATE TABLE IF NOT EXISTS edwpsc.epic_refdiagnosiscode
(
  diagnosiscodekey INT64 NOT NULL,
  diagnosiscodename STRING,
  diagnosiscode STRING,
  diagnosisicd10 STRING,
  dxid STRING,
  diagnosisexternalid STRING,
  deleteflag INT64,
  regionkey INT64,
  sourceaprimarykey STRING,
  sourceaprimarykeylastupdated DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (diagnosiscodekey) NOT ENFORCED
)
;