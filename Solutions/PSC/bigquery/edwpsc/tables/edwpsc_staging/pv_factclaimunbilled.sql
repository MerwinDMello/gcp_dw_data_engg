CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_factclaimunbilled
(
  regionkey INT64,
  coid STRING,
  practice STRING,
  claimkey INT64,
  claimnumber INT64,
  unbilledstatuskey INT64,
  claimunbilledstatusinrhinventory INT64,
  claimunbilledstatusrhholdcode STRING,
  claimunbilledstatusedinohold INT64,
  claimunbilledstatusminsubmissiondate DATE,
  claimunbilledstatusmaxsubmissiondate DATE,
  claimunbilledstatusclaimstatus STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  rhunbilledcategory STRING,
  claimstatusowner STRING,
  dwlastupdatedatetime DATETIME
)
;