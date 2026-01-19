CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_stgeratrans
(
  practice STRING NOT NULL,
  era_date DATETIME NOT NULL,
  era_num INT64 NOT NULL,
  receiver_name STRING NOT NULL,
  receiver_id STRING NOT NULL,
  sender_name STRING NOT NULL,
  sender_id STRING NOT NULL,
  isa_ctrl_num STRING NOT NULL,
  environment STRING NOT NULL,
  check_nums STRING NOT NULL,
  era_post_date DATETIME NOT NULL,
  parsed_report STRING NOT NULL,
  posted_report STRING NOT NULL,
  exception_report STRING NOT NULL,
  last_upd_userid STRING NOT NULL,
  last_upd_datetime DATETIME NOT NULL,
  eratranspk STRING NOT NULL,
  docrootid INT64,
  docpath STRING,
  erafilename STRING,
  regionkey INT64 NOT NULL,
  dwlastupdatedatetime DATETIME,
  sourcephysicaldeleteflag BOOL
)
;