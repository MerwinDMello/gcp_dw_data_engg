CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_stgerafileclaimlineallowed
(
  allowed_id INT64 NOT NULL,
  claimline_id INT64,
  claim_id INT64,
  payer_id INT64,
  fileid INT64,
  bpr_id INT64,
  trn_id INT64,
  allowed01 STRING,
  allowedsegment STRING,
  inserteddtm DATETIME NOT NULL,
  gs_id INT64,
  dwlastupdatedatetime DATETIME NOT NULL
)
;