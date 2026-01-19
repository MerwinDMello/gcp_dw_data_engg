CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_stginvoiceworklist
(
  invoiceworklistid INT64,
  practice STRING,
  arctrlnum INT64 NOT NULL,
  invnum INT64 NOT NULL,
  clinic STRING,
  svcdate DATETIME,
  invdate DATETIME,
  visittype STRING,
  patnum INT64,
  patname STRING,
  payernum INT64,
  payerclass STRING,
  payertype STRING,
  payername STRING,
  crgbalance NUMERIC(31, 2),
  actiondate DATETIME,
  worklistpk STRING,
  releaseflag STRING,
  displayui BOOL,
  regionkey INT64,
  inserteddtm DATETIME,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcephysicaldeleteflag INT64
)
;