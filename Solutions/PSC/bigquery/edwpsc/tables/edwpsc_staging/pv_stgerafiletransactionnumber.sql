CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_stgerafiletransactionnumber
(
  trn_id INT64 NOT NULL,
  bpr_id INT64,
  fileid INT64,
  trn01 STRING,
  trn02 STRING,
  trn03 STRING,
  trn04 STRING,
  trn05 STRING,
  trn06 STRING,
  trn07 STRING,
  trn08 STRING,
  trn09 STRING,
  trn10 STRING,
  trnsegment STRING,
  inserteddtm DATETIME NOT NULL,
  gs_id INT64,
  dwlastupdatedatetime DATETIME NOT NULL
)
;