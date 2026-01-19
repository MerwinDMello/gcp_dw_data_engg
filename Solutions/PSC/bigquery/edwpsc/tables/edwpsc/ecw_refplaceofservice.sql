CREATE TABLE IF NOT EXISTS edwpsc.ecw_refplaceofservice
(
  poskey INT64 NOT NULL,
  poscategorykey INT64 NOT NULL,
  posname STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  paymentrate STRING,
  PRIMARY KEY (poskey) NOT ENFORCED
)
;