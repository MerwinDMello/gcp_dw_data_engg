CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_cashvaluecurrentclaim
(
  claimkey INT64,
  claimnumber INT64,
  regionkey INT64,
  claimbalancecashvalueamt NUMERIC(33, 4),
  dwlastupdatedatetime DATETIME NOT NULL
)
;