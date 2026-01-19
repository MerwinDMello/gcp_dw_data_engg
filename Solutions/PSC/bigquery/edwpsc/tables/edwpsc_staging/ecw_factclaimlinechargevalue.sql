CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factclaimlinechargevalue
(
  claimlinechargevaluekey INT64 NOT NULL,
  processeddate DATETIME NOT NULL,
  processedchargevalueprioritynum INT64,
  processedchargevaluetype STRING,
  processedlastrunflag INT64 NOT NULL,
  chargevaluekey INT64,
  claimkey INT64,
  claimnumber INT64,
  coid STRING,
  claimlinechargekey INT64,
  practiceid STRING,
  adjcode STRING,
  cptbalance NUMERIC(33, 4),
  dwlastupdatedatetime DATETIME NOT NULL
)
;