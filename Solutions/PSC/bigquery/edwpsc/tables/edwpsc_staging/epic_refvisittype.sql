CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refvisittype
(
  visittypekey INT64 NOT NULL,
  visittypename STRING,
  visittypeabbr STRING,
  visittypedescription STRING,
  visittyperequiredclaim INT64,
  visittyperequiredcopay INT64,
  visittypepregnancyvisit INT64,
  visittypeactiveflag INT64,
  visittypeorthovisit INT64,
  visittypeobgynvisit INT64,
  visittypeisvisit INT64,
  visittypewebvisit INT64,
  visittypephysicaltherapyvisit INT64,
  sourceprimarykeyvalue STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  regionkey INT64
)
;