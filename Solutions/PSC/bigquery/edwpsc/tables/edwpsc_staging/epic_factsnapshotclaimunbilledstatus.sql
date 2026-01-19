CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_factsnapshotclaimunbilledstatus
(
  unbilledstatussnapshotkey INT64 NOT NULL,
  monthid INT64,
  snapshotdate DATE,
  claimkey INT64 NOT NULL,
  claimnumber INT64 NOT NULL,
  regionkey INT64,
  coid STRING,
  patientinternalid INT64,
  visitnumber INT64,
  unbilledstatuskey INT64,
  claimunbilledstatusinrhinventory INT64,
  claimunbilledstatusrhholdcode STRING,
  claimunbilledstatusedinohold INT64,
  claimunbilledstatusminsubmissiondate DATE,
  claimunbilledstatusmaxsubmissiondate DATE,
  claimunbilledstatusclaimstatus STRING,
  insertedby STRING,
  inserteddtm DATETIME
)
;