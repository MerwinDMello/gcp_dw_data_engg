CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factencounterdiagnosismeditechexpanse
(
  encounterdiagnosismtxkey INT64 NOT NULL,
  regionkey INT64,
  encounterkey INT64,
  diagnosiscodekey INT64,
  diagnosiscode STRING,
  diagnosistransnum INT64,
  diagnosisorder INT64,
  diagnosisbchid STRING,
  deleteflag INT64,
  sourcelastupdateddate DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;