CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refuser_history
(
  userkey INT64 NOT NULL,
  userfirstname STRING,
  userlastname STRING,
  usermiddlename STRING,
  username STRING NOT NULL,
  userprimaryservicelocation INT64,
  usertype INT64,
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
  sysstarttime DATETIME NOT NULL,
  sysendtime DATETIME NOT NULL
)
;