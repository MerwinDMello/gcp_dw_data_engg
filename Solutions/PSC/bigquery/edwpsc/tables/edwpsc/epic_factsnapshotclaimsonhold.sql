CREATE TABLE IF NOT EXISTS edwpsc.epic_factsnapshotclaimsonhold
(
  monthid INT64 NOT NULL,
  snapshotdate DATE,
  loaddatekey DATE,
  claimkey INT64,
  claimnumber INT64,
  regionkey INT64,
  holdcodekey INT64,
  holdcode INT64,
  holdcodename STRING,
  holdfromdatekey DATE,
  holdtodatekey DATE,
  daysonhold INT64,
  sourcesystemcode STRING NOT NULL,
  insertedby NUMERIC(29) NOT NULL,
  inserteddtm DATETIME NOT NULL
)
PARTITION BY snapshotdate
;