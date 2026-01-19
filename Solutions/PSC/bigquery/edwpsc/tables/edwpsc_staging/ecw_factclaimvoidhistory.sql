CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factclaimvoidhistory
(
  claimvoidhistorykey INT64 NOT NULL,
  oldclaimnumber INT64,
  oldclaimkey INT64,
  reversedclaimnumber INT64,
  reversedclaimkey INT64,
  newclaimnumber INT64,
  newclaimkey INT64,
  sourceprimarykeyvalue INT64 NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  regionkey INT64,
  coid STRING,
  archivedrecord STRING NOT NULL,
  PRIMARY KEY (claimvoidhistorykey) NOT ENFORCED
)
;