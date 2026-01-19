CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_reftypeofservice
(
  toskey INT64 NOT NULL,
  toscode STRING NOT NULL,
  tosname STRING,
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
  PRIMARY KEY (toskey) NOT ENFORCED
)
;