CREATE TABLE IF NOT EXISTS edwpsc.ecw_refcoidconfiguration_history
(
  coidconfigurationkey INT64 NOT NULL,
  coid STRING NOT NULL,
  deptcode STRING NOT NULL,
  coidconfigurationproviderkey INT64 NOT NULL,
  coidconfigurationfacilitykey INT64 NOT NULL,
  coidconfigurationpracticekey INT64 NOT NULL,
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