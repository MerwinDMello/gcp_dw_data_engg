CREATE TABLE IF NOT EXISTS edwpsc.ecw_rprticcreleasedclaims
(
  snapshotdate DATE,
  coid STRING,
  claimnumber INT64,
  rhclaimhistorysubmissiondatekey STRING,
  rhclaimhistorybridgefilenumber STRING,
  rhclaimhistorypatientcontrolnbr STRING,
  rhclaimhistoryholdcode STRING,
  rhclaimhistoryreleaseddatekey DATE,
  rhclaimhistorytotalclaimamount NUMERIC(33, 4),
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  claimkey INT64,
  regionkey INT64,
  dwlastupdatedatetime DATETIME
)
PARTITION BY snapshotdate
;