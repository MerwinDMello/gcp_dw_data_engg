CREATE TABLE IF NOT EXISTS edwpsc.ecw_refiplanmeditechexpanse
(
  iplankey INT64 NOT NULL,
  regionkey INT64,
  iplanname STRING,
  iplanprimaryaddressline1 STRING,
  iplanprimaryaddressline2 STRING,
  iplanprimarygeographykey INT64,
  iplangroupid STRING,
  deleteflag INT64,
  sourceprimarykeyvalue STRING,
  sourcearecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (iplankey) NOT ENFORCED
)
;