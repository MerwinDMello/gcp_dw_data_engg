CREATE TABLE IF NOT EXISTS edwpsc.pv_factsnapshotclaimunbilledstatus
(
  unbilledstatussnapshotkey INT64 NOT NULL,
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
  insertedby STRING NOT NULL,
  inserteddtm DATETIME NOT NULL,
  modifiedby STRING NOT NULL,
  modifieddtm DATETIME NOT NULL,
  snapshotdate DATE,
  rhunbilledcategory STRING,
  claimstatusowner STRING,
  dwlastupdatedatetime DATETIME NOT NULL
)
PARTITION BY snapshotdate
;