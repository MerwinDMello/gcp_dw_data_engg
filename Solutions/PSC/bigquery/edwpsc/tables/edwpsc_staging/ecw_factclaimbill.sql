CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factclaimbill
(
  billkey INT64 NOT NULL,
  claimkey INT64,
  claimnumber INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  coid STRING NOT NULL,
  coidconfigurationkey INT64 NOT NULL,
  servicingproviderkey INT64 NOT NULL,
  claimpayer1iplankey INT64 NOT NULL,
  facilitykey INT64 NOT NULL,
  billtypekey INT64 NOT NULL,
  iplankey INT64 NOT NULL,
  billmessage STRING NOT NULL,
  batchnumber INT64 NOT NULL,
  billdatekey DATE NOT NULL,
  billtime TIME NOT NULL,
  sourceprimarykeyvalue INT64 NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  archivedrecord STRING NOT NULL
)
;