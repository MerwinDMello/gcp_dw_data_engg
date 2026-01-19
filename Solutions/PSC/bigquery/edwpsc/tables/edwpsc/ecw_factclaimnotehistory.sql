CREATE TABLE IF NOT EXISTS edwpsc.ecw_factclaimnotehistory
(
  claimstatusnotekey INT64 NOT NULL,
  claimkey INT64,
  claimnumber INT64,
  regionkey INT64,
  coid STRING,
  claimnote STRING,
  claimnotecreateddate DATE,
  claimnotecreatedtime TIME,
  claimnotecreatedbyuserkey INT64,
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