CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_juncregioncoidpractice
(
  juncregioncoidpracticekey INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  coid STRING NOT NULL,
  practicekey INT64 NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncregioncoidpracticekey) NOT ENFORCED
)
;