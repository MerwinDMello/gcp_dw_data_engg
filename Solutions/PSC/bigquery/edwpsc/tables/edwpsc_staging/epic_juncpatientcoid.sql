CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_juncpatientcoid
(
  juncpatientcoidkey INT64 NOT NULL,
  patientkey INT64 NOT NULL,
  coid STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncpatientcoidkey) NOT ENFORCED
)
;