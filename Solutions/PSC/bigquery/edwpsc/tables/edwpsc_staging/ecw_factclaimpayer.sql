CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factclaimpayer
(
  claimpayerkey INT64 NOT NULL,
  claimkey INT64 NOT NULL,
  claimnumber INT64 NOT NULL,
  coid STRING NOT NULL,
  seqnumber INT64 NOT NULL,
  payeriplankey INT64 NOT NULL,
  payergroupnumber STRING,
  payergroupname STRING,
  payersubscribernumber STRING,
  payerclaimindicator STRING,
  payerliabilityowner STRING,
  payersourcechangedflag INT64 NOT NULL,
  payersourceprimarykeyvalue INT64,
  payersourcetablelastupdated DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  priorauthno STRING,
  regionkey INT64,
  arclaimflag INT64,
  archivedrecord STRING NOT NULL
)
;