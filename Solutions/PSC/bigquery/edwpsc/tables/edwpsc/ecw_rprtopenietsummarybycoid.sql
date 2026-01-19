CREATE TABLE IF NOT EXISTS edwpsc.ecw_rprtopenietsummarybycoid
(
  snapshotdate DATE,
  groupname STRING,
  divisionname STRING,
  marketname STRING,
  coid STRING,
  coidname STRING,
  claimnumber INT64,
  claimbalance INT64,
  errorcount INT64,
  claimkey INT64
)
PARTITION BY snapshotdate
;