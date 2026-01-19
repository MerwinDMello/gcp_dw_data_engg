CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_facttransactionspaymentheader
(
  transactionpaymentheaderkey INT64 NOT NULL,
  regionkey INT64,
  paymentheaderdate DATE,
  paymentheadertime TIME,
  paymentheaderuserkey INT64,
  paymentheaderuserid INT64,
  transactionflag STRING,
  paymentid INT64,
  paymentheaderamt NUMERIC(31, 2),
  paymentheaderdescription STRING,
  paymentheadermodifieddate DATETIME,
  sourceaprimarykeyvalue INT64,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;