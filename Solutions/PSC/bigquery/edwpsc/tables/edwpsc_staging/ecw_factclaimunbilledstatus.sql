CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factclaimunbilledstatus
(
  claimkey INT64 NOT NULL,
  claimnumber INT64 NOT NULL,
  unbilledstatuskey INT64,
  claimunbilledstatusinrhinventory INT64,
  claimunbilledstatusrhholdcode STRING,
  claimunbilledstatusedinohold INT64,
  claimunbilledstatusminsubmissiondate DATE,
  claimunbilledstatusmaxsubmissiondate DATE,
  claimunbilledstatusclaimstatus STRING,
  coid STRING,
  regionkey INT64,
  inserteddtm DATETIME,
  rhunbilledcategory STRING,
  holdcategory STRING,
  claimstatusowner STRING,
  PRIMARY KEY (claimkey) NOT ENFORCED
)
;