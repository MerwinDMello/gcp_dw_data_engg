CREATE TABLE IF NOT EXISTS edwpsc.pv_juncdiagnosiscodecoid
(
  juncdiagnosiscodecoidkey INT64 NOT NULL,
  diagnosiscodekey INT64 NOT NULL,
  coid STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncdiagnosiscodecoidkey) NOT ENFORCED
)
;