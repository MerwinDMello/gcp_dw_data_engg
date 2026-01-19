CREATE TABLE IF NOT EXISTS edwpsc.ecw_refclaimstatus_history
(
  claimstatuskey INT64 NOT NULL,
  claimstatusname STRING NOT NULL,
  claimstatusshortname STRING NOT NULL,
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