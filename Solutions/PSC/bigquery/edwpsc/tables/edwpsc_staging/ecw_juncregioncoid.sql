CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_juncregioncoid
(
  juncregioncoidkey INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  coid STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncregioncoidkey) NOT ENFORCED
)
;