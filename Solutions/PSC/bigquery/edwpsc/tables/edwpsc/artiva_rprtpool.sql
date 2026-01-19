CREATE TABLE IF NOT EXISTS edwpsc.artiva_rprtpool
(
  snapshotdate DATE,
  claimkey INT64,
  claimnumber INT64,
  totalbalanceamt NUMERIC(33, 4),
  artivaliabilitypool INT64,
  poolname STRING,
  artivaliabilitylastrevieweddate DATE,
  artivaliabilitylastworkeddate DATE,
  artivaliabilityfollowupdate DATE,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
PARTITION BY snapshotdate
;