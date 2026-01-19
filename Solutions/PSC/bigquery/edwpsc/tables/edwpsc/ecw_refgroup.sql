CREATE TABLE IF NOT EXISTS edwpsc.ecw_refgroup
(
  groupkey INT64 NOT NULL,
  groupname STRING NOT NULL,
  groupisvisibleonfactclaim INT64 NOT NULL,
  sourceprimarykeyvalue INT64 NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  groupcode STRING,
  deleteflag INT64,
  coidstatflag INT64,
  ppmsflag INT64,
  sysstarttime DATETIME NOT NULL,
  sysendtime DATETIME NOT NULL,
  PRIMARY KEY (groupkey) NOT ENFORCED
)
;