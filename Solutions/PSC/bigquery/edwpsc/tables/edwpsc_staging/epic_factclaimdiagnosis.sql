CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_factclaimdiagnosis
(
  claimdiagnosiskey INT64 NOT NULL,
  claimkey INT64 NOT NULL,
  claimnumber INT64 NOT NULL,
  visitnumber INT64,
  regionkey INT64 NOT NULL,
  coid STRING NOT NULL,
  coidconfigurationkey INT64 NOT NULL,
  servicingproviderkey INT64 NOT NULL,
  claimpayer1iplankey INT64 NOT NULL,
  facilitykey INT64 NOT NULL,
  diagnosiscodekey INT64 NOT NULL,
  primarycode INT64 NOT NULL,
  icdorder INT64 NOT NULL,
  deleteflag INT64,
  sourceaprimarykeyvalue INT64 NOT NULL,
  sourcearecordlastupdated DATETIME NOT NULL,
  sourcebprimarykeyvalue STRING NOT NULL,
  sourcebrecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (claimdiagnosiskey) NOT ENFORCED
)
;