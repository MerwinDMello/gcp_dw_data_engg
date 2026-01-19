CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refdivision
(
  divisionkey INT64 NOT NULL,
  divisionname STRING NOT NULL,
  groupkey INT64 NOT NULL,
  divisionisvisibleonfactclaim INT64 NOT NULL,
  sourceprimarykeyvalue INT64 NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  divisioncode STRING,
  deleteflag INT64,
  coidstatflag INT64,
  ppmsflag INT64,
  sysstarttime DATETIME NOT NULL,
  sysendtime DATETIME NOT NULL,
  PRIMARY KEY (divisionkey) NOT ENFORCED
)
;