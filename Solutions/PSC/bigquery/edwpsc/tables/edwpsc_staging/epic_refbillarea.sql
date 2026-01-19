CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refbillarea
(
  billareakey INT64 NOT NULL,
  billareaname STRING,
  billareaabbr STRING,
  billareaglprefix STRING,
  billareaid STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  billareafederaltaxid STRING,
  PRIMARY KEY (billareakey) NOT ENFORCED
)
;