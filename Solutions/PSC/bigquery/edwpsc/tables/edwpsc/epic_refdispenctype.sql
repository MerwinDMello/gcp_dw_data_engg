CREATE TABLE IF NOT EXISTS edwpsc.epic_refdispenctype
(
  dispenctypekey INT64 NOT NULL,
  dispenctypename STRING,
  dispenctypetitle STRING,
  dispenctypeabbr STRING,
  dispenctypeinternalid STRING,
  dispenctypec STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (dispenctypekey) NOT ENFORCED
)
;