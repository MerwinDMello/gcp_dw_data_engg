CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_rprtpeclaimsonholdrollforward
(
  coid STRING,
  groupname STRING,
  divisionname STRING,
  marketname STRING,
  lob STRING,
  providerid INT64,
  providername STRING,
  filestatus STRING,
  amount NUMERIC(33, 4),
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  columntype STRING,
  claimcount INT64,
  snapshotdate DATE,
  dwlastupdatedatetime DATETIME
)
;