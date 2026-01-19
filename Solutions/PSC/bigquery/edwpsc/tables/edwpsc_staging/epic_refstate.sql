CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refstate
(
  statekey INT64 NOT NULL,
  stateabbr STRING,
  statename STRING,
  statec STRING,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (statekey) NOT ENFORCED
)
;