CREATE TABLE IF NOT EXISTS edwpsc.epic_refclaimstatus
(
  claimstatuskey INT64 NOT NULL,
  claimstatusname STRING NOT NULL,
  claimstatusshortname STRING NOT NULL,
  regionkey INT64,
  activityc INT64,
  sourceprimarykeyvalue INT64 NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  PRIMARY KEY (claimstatuskey) NOT ENFORCED
)
;