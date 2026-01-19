CREATE TABLE IF NOT EXISTS edwpsc.ecw_facterapayeridentification
(
  erapayeridentificationkey INT64 NOT NULL,
  claimkey INT64,
  claimnumber INT64 NOT NULL,
  n101 STRING,
  n102 STRING,
  n103 STRING,
  n104 STRING,
  n105 STRING,
  n106 STRING,
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
  regionkey INT64
)
;