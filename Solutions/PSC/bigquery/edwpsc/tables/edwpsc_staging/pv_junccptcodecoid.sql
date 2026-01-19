CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_junccptcodecoid
(
  junccptcodecoidkey INT64 NOT NULL,
  cptcodekey INT64 NOT NULL,
  coid STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (junccptcodecoidkey) NOT ENFORCED
)
;