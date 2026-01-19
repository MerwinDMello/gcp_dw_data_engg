CREATE TABLE IF NOT EXISTS edwpsc.ecw_reffacilitymeditechexpanse
(
  facilitykey INT64 NOT NULL,
  regionkey INT64,
  facilityname STRING NOT NULL,
  facilityaccountnumberprefix STRING,
  deleteflag INT64,
  sourceprimarykeyvalue STRING,
  sourcearecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (facilitykey) NOT ENFORCED
)
;