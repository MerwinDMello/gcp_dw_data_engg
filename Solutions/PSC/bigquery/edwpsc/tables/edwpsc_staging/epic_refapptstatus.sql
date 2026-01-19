CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refapptstatus
(
  apptstatuskey INT64 NOT NULL,
  apptstatusname STRING,
  apptstatustitle STRING,
  apptstatusabbr STRING,
  apptstatusinternalid STRING,
  apptstatusc STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (apptstatuskey) NOT ENFORCED
)
;