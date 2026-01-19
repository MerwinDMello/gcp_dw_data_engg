CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refvisittype_history
(
  visittypekey INT64 NOT NULL,
  visittypename STRING,
  visittypedescription STRING NOT NULL,
  visittyperequiredclaim INT64 NOT NULL,
  visittyperequiredcopay INT64 NOT NULL,
  visittypepregnancyvisit INT64 NOT NULL,
  visittypeactiveflag INT64 NOT NULL,
  visittypeorthovisit INT64 NOT NULL,
  visittypeobgynvisit INT64 NOT NULL,
  visittypeisvisit INT64 NOT NULL,
  visittypewebvisit INT64 NOT NULL,
  visittypephysicaltherapyvisit INT64 NOT NULL,
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