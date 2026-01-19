CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_factrh277response
(
  rh277responsekey INT64 NOT NULL,
  claimkey INT64,
  claimnumber INT64,
  regionkey INT64,
  coid STRING,
  importdatekey DATE,
  rh277responseclaimid STRING,
  rh277responsepatientcontrolnbr STRING,
  rh277responsedateimportedkey DATE,
  rh277responserespondingpayer STRING,
  rh277responsestatusctgycode STRING,
  rh277responsestatuscode STRING,
  rh277filename STRING,
  sourceprimarykeyvalue STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (rh277responsekey) NOT ENFORCED
)
;