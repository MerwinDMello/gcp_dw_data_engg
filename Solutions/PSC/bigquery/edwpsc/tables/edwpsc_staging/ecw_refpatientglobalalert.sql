CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refpatientglobalalert
(
  patientglobalalertkey INT64 NOT NULL,
  patientkey INT64,
  patientglobalalertcodekey INT64,
  patientglobalalertnote STRING,
  patientglobalalertpriority INT64,
  sourceprimarykeyvalue INT64 NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  regionkey INT64,
  PRIMARY KEY (patientglobalalertkey) NOT ENFORCED
)
;