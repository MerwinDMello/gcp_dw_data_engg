CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refadjustmentcode
(
  adjustmentcodekey INT64 NOT NULL,
  adjcode STRING NOT NULL,
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
  sap101 INT64 NOT NULL,
  PRIMARY KEY (adjustmentcodekey) NOT ENFORCED
)
;