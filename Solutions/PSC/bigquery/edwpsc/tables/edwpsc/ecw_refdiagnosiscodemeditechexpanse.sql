CREATE TABLE IF NOT EXISTS edwpsc.ecw_refdiagnosiscodemeditechexpanse
(
  diagnosiscodekey INT64 NOT NULL,
  diagnosiscode STRING NOT NULL,
  diagnosisname STRING NOT NULL,
  deleteflag INT64,
  sourceaprimarykeyvalue STRING,
  sourcearecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (diagnosiscodekey) NOT ENFORCED
)
;