CREATE TABLE IF NOT EXISTS edwpsc.ecw_rprticcsubcategoryerrors
(
  snapshotdate DATE,
  coid STRING,
  claimkey INT64,
  claimnumber INT64,
  subcategorydescription STRING,
  errorcount INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
PARTITION BY snapshotdate
;