CREATE TABLE IF NOT EXISTS edwpsc.ecw_factecwremarkcodeline
(
  ecwremarkcodelinekey INT64 NOT NULL,
  claimkey INT64,
  claimnumber INT64,
  claimlinechargeskey INT64,
  coid STRING,
  coidconfigurationkey INT64,
  servicingproviderkey INT64,
  claimpayer1iplankey INT64,
  claimlinepaymentskey INT64,
  regionkey INT64,
  remarkdetailid INT64,
  remarkcodetype STRING,
  remarkcode STRING,
  remarkdeleteflag INT64,
  sourceaprimarykey INT64,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (ecwremarkcodelinekey) NOT ENFORCED
)
;