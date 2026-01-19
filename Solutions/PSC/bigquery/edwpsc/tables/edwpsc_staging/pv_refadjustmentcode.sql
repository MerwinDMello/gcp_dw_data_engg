CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_refadjustmentcode
(
  adjustmentcodekey INT64 NOT NULL,
  adjcode STRING,
  adjname STRING NOT NULL,
  adjdescription STRING NOT NULL,
  adjustmentcategorykey INT64 NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  nonparflag INT64,
  billableflag INT64,
  adjsubcategoryname STRING,
  PRIMARY KEY (adjustmentcodekey) NOT ENFORCED
)
;