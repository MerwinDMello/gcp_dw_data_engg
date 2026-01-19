CREATE TABLE IF NOT EXISTS edwpsc.epic_factclaimsonhold
(
  claimsonholdkey INT64 NOT NULL,
  loaddatekey DATE,
  claimkey INT64,
  claimnumber INT64,
  regionkey INT64,
  holdcodekey INT64,
  holdcode INT64,
  holdcodename STRING,
  holdfromdatekey DATE,
  holdtodatekey DATE,
  daysonhold INT64,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (claimsonholdkey) NOT ENFORCED
)
;