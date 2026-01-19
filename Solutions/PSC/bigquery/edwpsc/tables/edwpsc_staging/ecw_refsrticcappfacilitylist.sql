CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refsrticcappfacilitylist
(
  facilitylistkey INT64 NOT NULL,
  uniqueidentifier INT64,
  contractcontrolnumber INT64,
  assignedvendor STRING,
  inventorytype STRING,
  coid INT64,
  regionkey INT64,
  siteid INT64,
  facilityname STRING,
  poolexecutivename STRING,
  pooldirectorname STRING,
  poolmanagername STRING,
  effectivedate DATE,
  termeddate DATE,
  sourceaprimarykeyvalue INT64,
  deleteflag INT64 NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME NOT NULL,
  modifiedby STRING,
  modifieddtm DATETIME
)
;