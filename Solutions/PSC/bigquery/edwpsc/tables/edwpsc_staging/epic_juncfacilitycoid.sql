CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_juncfacilitycoid
(
  juncfacilitycoidkey INT64 NOT NULL,
  facilitykey INT64 NOT NULL,
  coid STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncfacilitycoidkey) NOT ENFORCED
)
;