CREATE TABLE IF NOT EXISTS edwpsc.epic_factworkqueuepatienterrors
(
  regionkey INT64,
  cerid INT64,
  ruleid INT64,
  errormessage STRING,
  deferdate DATETIME,
  sourceaprimarykeyvalue INT64,
  sourcebprimarykeyvalue INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;