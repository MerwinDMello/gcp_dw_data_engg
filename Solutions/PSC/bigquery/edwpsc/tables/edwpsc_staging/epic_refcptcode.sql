CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refcptcode
(
  cptcodekey INT64 NOT NULL,
  cptcode STRING,
  cptname STRING,
  deleteflag INT64,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  category1code STRING,
  category2code STRING,
  PRIMARY KEY (cptcodekey) NOT ENFORCED
)
;