CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factsnapshotclaimunbilledstatus
(
  claimkey INT64 NOT NULL,
  monthid INT64,
  snapshotdate DATE,
  claimnumber INT64 NOT NULL,
  unbilledstatuskey INT64,
  claimunbilledstatusinrhinventory INT64,
  claimunbilledstatusrhholdcode STRING,
  claimunbilledstatusedinohold INT64,
  claimunbilledstatusminsubmissiondate DATE,
  claimunbilledstatusmaxsubmissiondate DATE,
  claimunbilledstatusclaimstatus STRING,
  coid STRING,
  rhunbilledcategory STRING,
  holdcategory STRING,
  claimstatusowner STRING
)
;