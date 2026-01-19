CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_stgerafilechecktypebpr
(
  bpr_id INT64 NOT NULL,
  fileid INT64,
  bpr01 STRING,
  bpr02 STRING,
  bpr03 STRING,
  bpr04 STRING,
  bpr05 STRING,
  bpr06 STRING,
  bpr07 STRING,
  bpr08 STRING,
  bpr09 STRING,
  bpr10 STRING,
  bpr11 STRING,
  bpr12 STRING,
  bpr13 STRING,
  bpr14 STRING,
  bpr15 STRING,
  bpr16 STRING,
  bprsegment STRING,
  inserteddtm DATETIME NOT NULL,
  gs_id INT64,
  dwlastupdatedatetime DATETIME NOT NULL
)
;