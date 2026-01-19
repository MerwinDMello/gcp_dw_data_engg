CREATE TABLE IF NOT EXISTS edwpsc.pv_stgerafileclaim
(
  claim_id INT64 NOT NULL,
  payer_id INT64,
  fileid INT64,
  bpr_id INT64,
  trn_id INT64,
  claim01 STRING,
  claim02 STRING,
  claim03 STRING,
  claim04 STRING,
  claim05 STRING,
  claim06 STRING,
  claim07 STRING,
  claim08 STRING,
  claim09 STRING,
  claim10 STRING,
  claim11 STRING,
  claim12 STRING,
  claim13 STRING,
  claim14 STRING,
  claimsegment STRING,
  inserteddtm DATETIME NOT NULL,
  gs_id INT64,
  dwlastupdatedatetime DATETIME NOT NULL
)
;