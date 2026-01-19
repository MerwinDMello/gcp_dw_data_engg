CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factusertouch
(
  usertouchkey INT64 NOT NULL,
  sourcesystemcode STRING NOT NULL,
  user34 STRING NOT NULL,
  regionkey INT64,
  encounterkey INT64,
  encounterid INT64,
  claimkey INT64,
  claimnumber INT64,
  touchdatekey DATE,
  touchbegindatetime DATETIME,
  touchenddatetime DATETIME,
  touchtype STRING,
  coid STRING,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  actiontablename STRING,
  actioncolumnname STRING,
  actionkey INT64,
  additionaldatatype1 STRING,
  additionaldatavalue1 STRING,
  additionaldatatype2 STRING,
  additionaldatavalue2 STRING,
  timeinminutes INT64,
  timegapminutes INT64
)
;