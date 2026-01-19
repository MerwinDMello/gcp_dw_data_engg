CREATE TABLE IF NOT EXISTS edwpsc.pv_stginvtrans
(
  practice STRING,
  inv_num INT64,
  line_num INT64,
  trans_num INT64,
  trans_date DATETIME,
  trans_type STRING,
  trans_amt NUMERIC(33, 4),
  trans_desc STRING,
  reason STRING,
  payment_num INT64,
  db_acnt STRING,
  cr_acnt STRING,
  closing_date DATETIME,
  crt_userid STRING,
  crt_datetime DATETIME,
  invtranspk STRING,
  regionkey INT64 NOT NULL,
  sourcephysicaldeleteflag INT64,
  dwlastupdatedatetime DATETIME
)
;