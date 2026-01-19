CREATE TABLE IF NOT EXISTS edwpsc_staging.ccu_rprtcoidprovidervariance
(
  ccucoidprovidervariancehistorykey INT64 NOT NULL,
  priorsnapshotdatekey DATE,
  snapshotdate DATE,
  recordtype STRING,
  coid STRING,
  providernpi STRING,
  providername STRING,
  priorstatus STRING,
  priorcentralizedstatus STRING,
  currstatus STRING,
  currcentralizedstatus STRING,
  result STRING,
  resultcount NUMERIC(35, 6),
  loaddate DATE,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;