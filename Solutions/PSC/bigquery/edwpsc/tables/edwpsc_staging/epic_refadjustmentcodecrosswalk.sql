CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refadjustmentcodecrosswalk
(
  adjcode STRING NOT NULL,
  regionkey INT64 NOT NULL,
  adjustmentcategorykey INT64 NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;