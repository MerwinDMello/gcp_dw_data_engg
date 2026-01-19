CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_stgnotes
(
  practice STRING NOT NULL,
  group_name STRING NOT NULL,
  type STRING NOT NULL,
  key_value STRING NOT NULL,
  subkey STRING NOT NULL,
  notes STRING NOT NULL,
  active STRING NOT NULL,
  last_upd_userid STRING NOT NULL,
  last_upd_datetime DATETIME NOT NULL,
  notespk STRING NOT NULL,
  notespk_txt STRING,
  patinfopk STRING,
  createdby STRING,
  createdon DATETIME,
  regionkey INT64 NOT NULL,
  sysstarttime DATETIME,
  inserteddtm DATETIME NOT NULL,
  modifieddtm DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcephysicaldeleteflag BOOL NOT NULL
)
;