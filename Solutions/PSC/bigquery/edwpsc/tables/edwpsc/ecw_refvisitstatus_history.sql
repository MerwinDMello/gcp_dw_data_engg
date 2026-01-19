CREATE TABLE IF NOT EXISTS edwpsc.ecw_refvisitstatus_history
(
  visitstatuskey INT64 NOT NULL,
  visitstatusname STRING NOT NULL,
  visitstatusdescription STRING NOT NULL,
  visitstatusnonbillable INT64 NOT NULL,
  sourceprimarykeyvalue INT64 NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  sysstarttime DATETIME NOT NULL,
  sysendtime DATETIME NOT NULL
)
;