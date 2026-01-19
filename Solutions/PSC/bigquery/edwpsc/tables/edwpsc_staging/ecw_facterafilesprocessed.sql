CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_facterafilesprocessed
(
  erafilesprocessedkey INT64 NOT NULL,
  claimkey INT64,
  claimnumber INT64 NOT NULL,
  sourcefilename STRING,
  filename STRING,
  sourcefilecreateddate DATETIME,
  datecreated DATETIME,
  transactionnumber STRING,
  trn03 STRING,
  iet_processed BOOL,
  posteddate DATETIME,
  sourceprimarykeyvalue INT64 NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deletedflag INT64,
  fullclaimnumber STRING,
  regionkey INT64,
  PRIMARY KEY (erafilesprocessedkey) NOT ENFORCED
)
;