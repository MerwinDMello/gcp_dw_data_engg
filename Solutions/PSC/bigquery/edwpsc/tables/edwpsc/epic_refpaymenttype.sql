CREATE TABLE IF NOT EXISTS edwpsc.epic_refpaymenttype
(
  paymenttypekey INT64 NOT NULL,
  paymenttype STRING,
  paymenttypename STRING,
  paymenttypedescription STRING,
  paymenttypeshortname STRING,
  paymenttypeactive INT64,
  procid STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (paymenttypekey) NOT ENFORCED
)
;