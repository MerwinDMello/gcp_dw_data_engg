CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refpaymentbatch
(
  batchkey INT64 NOT NULL,
  batchname STRING,
  batchdatekey INT64,
  batchopendate DATETIME,
  batchcloseddate DATETIME,
  batchtotalamt NUMERIC(33, 4),
  batchpostedamt NUMERIC(33, 4),
  batchstatus INT64,
  remitflag INT64,
  batchtypedesc STRING,
  batchnote STRING,
  createdbyuserkey INT64,
  closedbyuserkey INT64,
  regionkey INT64,
  deleteflag INT64,
  sourceaprimarykeyvalue INT64,
  sourcearecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (batchkey) NOT ENFORCED
)
;