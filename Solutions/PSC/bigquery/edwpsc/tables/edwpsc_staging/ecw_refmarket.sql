CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refmarket
(
  marketkey INT64 NOT NULL,
  marketname STRING NOT NULL,
  divisionkey INT64 NOT NULL,
  sourceprimarykeyvalue INT64 NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  marketcode STRING,
  deleteflag INT64,
  coidstatflag INT64,
  ppmsflag INT64,
  sysstarttime DATETIME NOT NULL,
  sysendtime DATETIME NOT NULL,
  PRIMARY KEY (marketkey) NOT ENFORCED
)
;