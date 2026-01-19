CREATE TABLE IF NOT EXISTS edwpsc.pv_factclaimlinechargeuserchangedate
(
  claimlinechargeuserchangedatekey INT64 NOT NULL,
  claimlinechargekey INT64 NOT NULL,
  sourcesystemtype STRING NOT NULL,
  crt_userid STRING,
  crt_datetime DATETIME,
  last_upd_userid STRING,
  last_upd_datetime DATETIME,
  payer_num INT64,
  crg_balance NUMERIC(31, 2),
  crg_amt NUMERIC(31, 2),
  proc_code STRING,
  modifier STRING,
  quantity INT64,
  cptdeleted STRING,
  regionkey INT64 NOT NULL,
  sysstarttime DATETIME,
  sysendtime DATETIME,
  sourceaprimarykeyvalue STRING NOT NULL,
  sourcebprimarykeyvalue STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;