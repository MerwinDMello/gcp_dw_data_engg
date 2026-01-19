CREATE TABLE IF NOT EXISTS edwpsc.pv_stgerafileclaimlinerarc
(
  rarc_id INT64 NOT NULL,
  claimline_id INT64,
  claim_id INT64,
  payer_id INT64,
  fileid INT64,
  bpr_id INT64,
  trn_id INT64,
  rarc01 STRING,
  rarc02_code STRING,
  rarcsegment STRING,
  inserteddtm DATETIME NOT NULL,
  gs_id INT64,
  dwlastupdatedatetime DATETIME NOT NULL
)
;