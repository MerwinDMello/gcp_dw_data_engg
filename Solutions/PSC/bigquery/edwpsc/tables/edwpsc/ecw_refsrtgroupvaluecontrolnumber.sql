CREATE TABLE IF NOT EXISTS edwpsc.ecw_refsrtgroupvaluecontrolnumber
(
  groupvaluecontrolnumberkey INT64 NOT NULL,
  primarygroupvalue STRING,
  secondarygroupvalue STRING,
  control1 INT64,
  control2 INT64,
  sourceaprimarykeyvalue INT64,
  deleteflag INT64 NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME NOT NULL,
  modifiedby STRING,
  modifieddtm DATETIME,
  primarygroupsourcesystem STRING,
  secondarygroupsourcesystem STRING
)
;