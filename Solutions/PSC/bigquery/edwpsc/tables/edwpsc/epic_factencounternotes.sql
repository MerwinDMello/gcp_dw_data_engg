CREATE TABLE IF NOT EXISTS edwpsc.epic_factencounternotes
(
  encounternoteskey INT64 NOT NULL,
  regionkey INT64,
  encounterkey INT64,
  encounterid INT64,
  claimkey INT64,
  visitnumber INT64,
  notetype STRING,
  notedatekey DATE,
  noteuserkey INT64,
  encounternote STRING,
  servicingproviderkey INT64,
  paytoproviderkey INT64,
  patientkey INT64,
  serviceareakey INT64,
  sourceaprimarykeyvalue INT64,
  sourcebprimarykeyvalue INT64,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;