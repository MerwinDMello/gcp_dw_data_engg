CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_facteraremarkcodes
(
  eraremarkcodeskey INT64 NOT NULL,
  claimkey INT64,
  claimnumber INT64 NOT NULL,
  lq01 STRING,
  lq02 STRING,
  segment STRING,
  sourcefilename STRING,
  sourcefilecreateddate DATETIME NOT NULL,
  seq INT64,
  datecreated DATETIME,
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
  PRIMARY KEY (eraremarkcodeskey) NOT ENFORCED
)
;