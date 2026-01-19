CREATE TABLE IF NOT EXISTS edwpsc.pv_stgerafilepayer
(
  payer_id INT64 NOT NULL,
  fileid INT64,
  bpr_id INT64,
  trn_id INT64,
  payer01 STRING,
  payer02 STRING,
  payer03 STRING,
  payer04 STRING,
  payer05 STRING,
  payer06 STRING,
  payer07 STRING,
  payer08 STRING,
  payer09 STRING,
  payer10 STRING,
  payersegment STRING,
  inserteddtm DATETIME NOT NULL,
  gs_id INT64,
  dwlastupdatedatetime DATETIME NOT NULL
)
;