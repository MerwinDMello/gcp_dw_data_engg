CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refpatientglobalalertcode
(
  patientglobalalertcodekey INT64 NOT NULL,
  patientglobalalertcodedesc STRING,
  sourceprimarykeyvalue INT64 NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  PRIMARY KEY (patientglobalalertcodekey) NOT ENFORCED
)
;