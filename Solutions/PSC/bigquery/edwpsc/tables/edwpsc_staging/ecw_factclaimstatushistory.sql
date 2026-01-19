CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factclaimstatushistory
(
  claimstatushistorykey INT64 NOT NULL,
  claimkey INT64 NOT NULL,
  claimnumber INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  coid STRING NOT NULL,
  claimstatushistorymessage STRING,
  claimstatushistoryfrom STRING,
  claimstatushistoryto STRING,
  claimstatushistorychangedate DATE,
  claimstatushistorychangetime TIME,
  claimstatushistorychangedbyuserkey INT64,
  sourceprimarykeyvalue INT64,
  sourcerecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  archivedrecord STRING NOT NULL
)
;