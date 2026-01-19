CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_refdiagnosiscode
(
  diagnosiscodekey INT64 NOT NULL,
  diagnosiscode STRING NOT NULL,
  diagnosisname STRING NOT NULL,
  sourceaprimarykeyvalue STRING,
  sourcearecordlastupdated DATETIME,
  sourcebprimarykeyvalue STRING,
  sourcebrecordlastupdated DATETIME,
  deleteflag INT64,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (diagnosiscodekey) NOT ENFORCED
)
;