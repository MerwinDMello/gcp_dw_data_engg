CREATE TABLE IF NOT EXISTS edwpsc.pk_factdiagnosiscodes
(
  pkdiagnosiscodekey INT64 NOT NULL,
  icd10code STRING,
  icd10codedescription STRING,
  icd10order INT64,
  deleteflag INT64,
  pkregionname STRING,
  sourcesystemcode STRING,
  sourceaprimarykeyvalue INT64,
  sourcebprimarykeyvalue INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;