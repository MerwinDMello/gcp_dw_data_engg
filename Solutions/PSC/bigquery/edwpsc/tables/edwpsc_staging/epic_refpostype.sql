CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refpostype
(
  postypekey INT64 NOT NULL,
  postypename STRING,
  postypetitle STRING,
  postypeabbr STRING,
  postypeinternalid STRING,
  postypec STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (postypekey) NOT ENFORCED
)
;