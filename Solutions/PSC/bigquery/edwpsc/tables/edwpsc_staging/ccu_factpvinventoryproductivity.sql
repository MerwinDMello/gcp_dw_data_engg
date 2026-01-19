CREATE TABLE IF NOT EXISTS edwpsc_staging.ccu_factpvinventoryproductivity
(
  ccupvinventoryproductivitykey INT64 NOT NULL,
  productivitydate DATE,
  user34 STRING,
  regionkey INT64,
  claimnumber STRING,
  practice STRING,
  claimstatuschangedto STRING,
  actiontime TIME,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;