CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_factuacdetail
(
  uacdetailkey INT64 NOT NULL,
  practice STRING,
  paymentnumber INT64 NOT NULL,
  transactiondate DATETIME,
  transactiontype STRING,
  transactionamount NUMERIC(33, 4),
  transactiondescription STRING,
  dbaccount STRING,
  craccount STRING,
  pkcreateduserid STRING,
  pkcreateddatetime DATETIME,
  uacdetailpk STRING,
  orguacnumber INT64,
  regionkey INT64 NOT NULL,
  sourceaprimarykey INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;