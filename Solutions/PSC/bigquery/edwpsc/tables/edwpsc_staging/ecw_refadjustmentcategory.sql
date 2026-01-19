CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refadjustmentcategory
(
  adjustmentcategorykey INT64 NOT NULL,
  adjcategoryname STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (adjustmentcategorykey) NOT ENFORCED
)
;