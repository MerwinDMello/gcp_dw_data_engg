CREATE TABLE IF NOT EXISTS edwpsc.ecw_juncservicingproviderfacilitycoid
(
  juncservicingproviderfacilitycoidkey INT64 NOT NULL,
  providerkey INT64,
  facilitykey INT64,
  coid STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncservicingproviderfacilitycoidkey) NOT ENFORCED
)
;