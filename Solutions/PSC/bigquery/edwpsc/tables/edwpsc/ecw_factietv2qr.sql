CREATE TABLE IF NOT EXISTS edwpsc.ecw_factietv2qr
(
  ietv2qrkey INT64 NOT NULL,
  rownumber INT64,
  correspondenceid INT64,
  content STRING,
  datecreated DATETIME,
  usercreated STRING,
  correspondencesubjectid STRING,
  claimcaseid STRING,
  claimnumber STRING,
  claimkey INT64,
  inbound BOOL,
  resolution STRING,
  resolutionid STRING,
  resolutiondate DATETIME,
  resolutionuserid STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  fullclaimnumber STRING,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (ietv2qrkey) NOT ENFORCED
)
;