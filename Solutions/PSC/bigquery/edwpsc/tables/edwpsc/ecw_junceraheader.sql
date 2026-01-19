CREATE TABLE IF NOT EXISTS edwpsc.ecw_junceraheader
(
  ecweraheaderkey INT64 NOT NULL,
  regionkey INT64,
  importedby INT64,
  importedbyuserkey INT64,
  importeddate DATETIME,
  rawfilename STRING,
  xml STRING,
  isa13 STRING,
  isa06 STRING,
  deletedby INT64,
  deletedbyuserkey INT64,
  deleteddate DATETIME,
  deletedreason STRING,
  trn02 STRING,
  trn03 STRING,
  ansi5010 INT64,
  deleteflag INT64,
  sourceprimarykeyvalue INT64,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;