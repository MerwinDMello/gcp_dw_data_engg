CREATE TABLE IF NOT EXISTS edwpsc.pv_factencounterdiagnosis
(
  encounterdiagnosiskey INT64 NOT NULL,
  regionkey INT64,
  coid STRING,
  practicename STRING,
  encounterkey INT64,
  encounterid INT64,
  diagnosiscodekey INT64,
  icd10 STRING,
  icd9 STRING,
  icdorder INT64,
  visitdate DATE,
  deleteflag INT64,
  sourceprimarykeyvalue STRING,
  sourcerecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;