CREATE TABLE IF NOT EXISTS edwpsc.ecw_juncregioncoidfacility
(
  juncregioncoidfacilitykey INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  coid STRING NOT NULL,
  facilitykey INT64 NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncregioncoidfacilitykey) NOT ENFORCED
)
;