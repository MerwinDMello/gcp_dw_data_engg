CREATE TABLE IF NOT EXISTS edwpsc.pk_refregion
(
  pkregionkey INT64 NOT NULL,
  pkregionname STRING NOT NULL,
  pkserverreferencename STRING NOT NULL,
  pkregionactiveflag INT64,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  regionname STRING,
  regionsourceconnectionstring STRING,
  regionsourceactive INT64,
  regiondbname STRING,
  regionservername STRING,
  regionssrsstagepackage STRING,
  regionlastrunstagecompleteflag INT64,
  regionlastrunstagefailuremessage STRING,
  regionredirectlogpath STRING,
  schemaname STRING,
  environmentid STRING,
  miscrunflag INT64,
  timezonedifference INT64,
  dwlastupdatedatetime DATETIME
)
;