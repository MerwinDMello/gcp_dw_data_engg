CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_factclaimpaymentcreditcardtransaction
(
  claimkey INT64 NOT NULL,
  logdetailpk STRING,
  creditcardtype STRING,
  ccsaletype STRING,
  createdcarddate DATETIME,
  reserveamt NUMERIC(31, 2),
  reserveamtused NUMERIC(31, 2),
  reserveamtremaining NUMERIC(31, 2),
  deletedflag INT64,
  declinedorcancelledflag INT64,
  declinedamt NUMERIC(31, 2),
  regionkey INT64,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  loadkey INT64
)
;