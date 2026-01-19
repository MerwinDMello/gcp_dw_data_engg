CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_reffacilityresource
(
  facilityresourcekey INT64 NOT NULL,
  facilityresourcename STRING NOT NULL,
  facilityresourceusertype INT64 NOT NULL,
  sourceaprimarykeyvalue INT64 NOT NULL,
  sourcearecordlastupdated DATETIME NOT NULL,
  sourcebrecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  PRIMARY KEY (facilityresourcekey) NOT ENFORCED
)
;