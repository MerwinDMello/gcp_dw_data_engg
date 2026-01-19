CREATE TABLE IF NOT EXISTS edwpsc.pv_juncfacilityresourcecoid
(
  juncfacilityresourcecoidkey INT64 NOT NULL,
  facilityresourcekey INT64 NOT NULL,
  coid STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncfacilityresourcecoidkey) NOT ENFORCED
)
;