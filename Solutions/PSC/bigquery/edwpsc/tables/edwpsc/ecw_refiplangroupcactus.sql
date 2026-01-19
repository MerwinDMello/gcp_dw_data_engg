CREATE TABLE IF NOT EXISTS edwpsc.ecw_refiplangroupcactus
(
  iplangroupcactuskey INT64 NOT NULL,
  iplangroupcactusname STRING NOT NULL,
  sourceprimarykeyvalue INT64 NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  PRIMARY KEY (iplangroupcactuskey) NOT ENFORCED
)
;