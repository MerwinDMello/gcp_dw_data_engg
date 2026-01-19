CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_factworkqueueprebilledithistory
(
  prebilledithistorykey INT64 NOT NULL,
  regionkey INT64,
  claimkey INT64,
  encounterkey INT64,
  encounterid INT64,
  activitystartdate DATE,
  activityenddate DATE,
  activityname STRING,
  workqueueid STRING,
  workqueuename STRING,
  sourceaprimarykeyvalue INT64 NOT NULL,
  sourcebprimarykeyvalue INT64 NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;