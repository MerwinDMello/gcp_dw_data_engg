CREATE TABLE IF NOT EXISTS edwpsc.ecw_refsrtgroupvaluedepartment
(
  groupvaluedeptkey INT64 NOT NULL,
  lob STRING,
  groupvalue STRING,
  department INT64,
  sourceaprimarykeyvalue INT64,
  deleteflag INT64 NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME NOT NULL,
  modifiedby STRING,
  modifieddtm DATETIME
)
;