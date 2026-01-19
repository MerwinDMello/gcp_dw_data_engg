CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_stgpaymentbatch
(
  batch_num INT64 NOT NULL,
  practice STRING NOT NULL,
  clinic STRING NOT NULL,
  deposit_date DATETIME NOT NULL,
  category STRING,
  total_amt NUMERIC(31, 2) NOT NULL,
  remain_amt NUMERIC(31, 2) NOT NULL,
  description STRING NOT NULL,
  crt_userid STRING NOT NULL,
  lock_by STRING NOT NULL,
  last_upd_userid STRING NOT NULL,
  last_upd_datetime DATETIME NOT NULL,
  erapostnum INT64 NOT NULL,
  paymentbatchpk STRING NOT NULL,
  crt_datetime DATETIME,
  regionkey INT64 NOT NULL,
  sourcephysicaldeleteflag INT64,
  dwlastupdatedatetime DATETIME NOT NULL
)
;