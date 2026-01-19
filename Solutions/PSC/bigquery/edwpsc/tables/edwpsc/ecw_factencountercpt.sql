CREATE TABLE IF NOT EXISTS edwpsc.ecw_factencountercpt
(
  encountercptkey INT64 NOT NULL,
  regionkey INT64,
  coid STRING,
  encounterkey INT64,
  encounterid INT64,
  cptcodekey INT64,
  cptcode STRING,
  cptunits INT64,
  cptorder INT64,
  visitdate DATE,
  primaryflag INT64,
  deleteflag INT64,
  sourceprimarykeyvalue STRING,
  sourcerecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  archivedrecord STRING NOT NULL
)
;