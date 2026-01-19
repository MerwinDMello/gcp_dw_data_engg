CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_factrh1301
(
  rh1301key INT64 NOT NULL,
  claimkey INT64,
  claimnumber INT64 NOT NULL,
  regionkey INT64,
  coid STRING,
  importdatekey DATE,
  rh1301claimid STRING,
  rh1301claimdatekey DATE,
  rh1301totalamt NUMERIC(33, 4),
  rh1301insurancebilledname STRING,
  rh1301errorfieldname STRING,
  rh1301errorindex STRING,
  rh1301errordata STRING,
  rh1301errordescription STRING,
  rh1301stmtthrudatekey DATE,
  rh1301filename STRING,
  sourceprimarykeyvalue STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;