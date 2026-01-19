CREATE TABLE IF NOT EXISTS edwpsc.epic_refadjustmentcode
(
  adjustmentcodekey INT64 NOT NULL,
  adjcode STRING,
  adjname STRING,
  adjdescription STRING,
  adjshortname STRING,
  adjactive INT64,
  procid STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  adjustmentcategorykey INT64,
  nonparflag INT64,
  billableflag INT64,
  adjsubcategoryname STRING,
  PRIMARY KEY (adjustmentcodekey) NOT ENFORCED
)
;