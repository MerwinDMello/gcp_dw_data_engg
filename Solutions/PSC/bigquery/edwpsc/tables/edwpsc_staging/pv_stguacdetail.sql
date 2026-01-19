CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_stguacdetail
(
  practice STRING NOT NULL,
  uacdetail_num INT64 NOT NULL,
  payment_num INT64 NOT NULL,
  trans_date DATETIME NOT NULL,
  trans_type STRING NOT NULL,
  trans_amt NUMERIC(33, 4) NOT NULL,
  trans_desc STRING NOT NULL,
  db_acnt STRING NOT NULL,
  cr_acnt STRING NOT NULL,
  crt_userid STRING NOT NULL,
  crt_datetime DATETIME NOT NULL,
  uacdetailpk STRING NOT NULL,
  org_uac_num INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  inserteddtm DATETIME,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcephysicaldeleteflag INT64
)
;