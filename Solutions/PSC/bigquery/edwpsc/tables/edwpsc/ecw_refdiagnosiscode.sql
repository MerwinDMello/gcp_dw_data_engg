CREATE TABLE IF NOT EXISTS edwpsc.ecw_refdiagnosiscode
(
  diagnosiscodekey INT64 NOT NULL,
  diagnosiscode STRING NOT NULL,
  diagnosisname STRING NOT NULL,
  sourceaprimarykeyvalue INT64,
  sourcearecordlastupdated DATETIME,
  sourcebprimarykeyvalue INT64,
  sourcebrecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  PRIMARY KEY (diagnosiscodekey) NOT ENFORCED
)
;