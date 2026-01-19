CREATE TABLE IF NOT EXISTS edwpsc.epic_refinvstatus
(
  invstatuskey INT64 NOT NULL,
  invstatusname STRING,
  invstatustitle STRING,
  invstatusabbr STRING,
  invstatusinternalid STRING,
  invstatusc STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (invstatuskey) NOT ENFORCED
)
;