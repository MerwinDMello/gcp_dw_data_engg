CREATE TABLE IF NOT EXISTS edwpsc.epic_juncpracticecoid
(
  juncpracticecoid INT64 NOT NULL,
  practicekey INT64 NOT NULL,
  coid STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncpracticecoid) NOT ENFORCED
)
;