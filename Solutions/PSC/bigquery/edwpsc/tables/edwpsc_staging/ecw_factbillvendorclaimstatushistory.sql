CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factbillvendorclaimstatushistory
(
  billvendorclaimstatushistorykey INT64 NOT NULL,
  sourceprimarykeyvalue INT64,
  fullclaimnumber STRING NOT NULL,
  claimkey INT64,
  tracenumber STRING NOT NULL,
  patienticn STRING NOT NULL,
  claimtotal NUMERIC(33, 4) NOT NULL,
  category277 STRING NOT NULL,
  status277 STRING NOT NULL,
  statdate DATETIME,
  reporttype STRING NOT NULL,
  processdate DATETIME,
  payid STRING NOT NULL,
  paysubid STRING NOT NULL,
  claimrank INT64 NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  addstatus STRING,
  delchrgloop STRING,
  trackstat STRING,
  stc12addstat STRING
)
;