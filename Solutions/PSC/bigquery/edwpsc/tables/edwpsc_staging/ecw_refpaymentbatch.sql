CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refpaymentbatch
(
  batchkey INT64 NOT NULL,
  batchname STRING NOT NULL,
  batchdatekey INT64,
  sourceprimarykeyvalue INT64 NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  regionkey INT64,
  PRIMARY KEY (batchkey) NOT ENFORCED
)
;