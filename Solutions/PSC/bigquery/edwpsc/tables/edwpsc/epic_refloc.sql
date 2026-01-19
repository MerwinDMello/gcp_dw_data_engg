CREATE TABLE IF NOT EXISTS edwpsc.epic_refloc
(
  lockey INT64 NOT NULL,
  locname STRING,
  locabbr STRING,
  locglprefix STRING,
  locservareaid STRING,
  serviceareakey INT64,
  deleteflag INT64,
  locpostype STRING,
  locposcode STRING,
  locfacilityid STRING,
  locid STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (lockey) NOT ENFORCED
)
;