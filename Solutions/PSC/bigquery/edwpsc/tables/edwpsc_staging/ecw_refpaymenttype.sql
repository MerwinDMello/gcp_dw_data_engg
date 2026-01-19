CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refpaymenttype
(
  paymenttypekey INT64 NOT NULL,
  paymenttypename STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (paymenttypekey) NOT ENFORCED
)
;