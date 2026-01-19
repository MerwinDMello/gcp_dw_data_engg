CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factrh277response
(
  rh277responsekey INT64 NOT NULL,
  claimkey INT64,
  claimnumber INT64,
  coid STRING,
  importdatekey DATE,
  rh277responseclaimid STRING,
  rh277responsepatientcontrolnbr STRING,
  rh277responsedateimportedkey DATE,
  rh277responserespondingpayer STRING,
  rh277responsestatusctgycode STRING,
  rh277responsestatuscode STRING,
  sourceprimarykeyvalue INT64 NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  fullclaimnumber STRING,
  regionkey INT64,
  PRIMARY KEY (rh277responsekey) NOT ENFORCED
)
;