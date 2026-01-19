CREATE TABLE IF NOT EXISTS edwpsc.epic_reftos
(
  toskey INT64 NOT NULL,
  tosname STRING,
  tostitle STRING,
  tosabbr STRING,
  tosinternalid STRING,
  tosc STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (toskey) NOT ENFORCED
)
;