CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factclaimpayerliabilitychange
(
  claimpayerliabilitychangekey INT64 NOT NULL,
  regionid INT64,
  claimnumber INT64,
  billedtoid INT64,
  billedtoidtype INT64,
  transferhistorydate DATE,
  transferhistorytime TIME,
  userid STRING,
  transferhistorymodifieddate DATETIME,
  userkey INT64,
  claimkey INT64,
  encounterkey INT64,
  encounterid INT64,
  claimpayerkey INT64,
  sourceaprimarykeyvalue INT64 NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  archivedrecord STRING NOT NULL
)
;