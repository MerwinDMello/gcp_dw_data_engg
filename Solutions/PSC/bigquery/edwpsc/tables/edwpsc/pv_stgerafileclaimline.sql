CREATE TABLE IF NOT EXISTS edwpsc.pv_stgerafileclaimline
(
  claimline_id INT64 NOT NULL,
  claim_id INT64,
  payer_id INT64,
  fileid INT64,
  bpr_id INT64,
  trn_id INT64,
  claimlinecpt STRING,
  claimlinemod1 STRING,
  claimlinemod2 STRING,
  claimlinemod3 STRING,
  claimlinemod4 STRING,
  claimline02 STRING,
  claimline03 STRING,
  claimline04 STRING,
  claimline05 STRING,
  claimline06 STRING,
  claimline07 STRING,
  claimlinesegment STRING,
  inserteddtm DATETIME NOT NULL,
  gs_id INT64,
  dwlastupdatedatetime DATETIME NOT NULL
)
;