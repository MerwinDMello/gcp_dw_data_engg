CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refregion
(
  regionkey INT64 NOT NULL,
  regionname STRING,
  regionsystemname STRING NOT NULL,
  regiondbsnapshotname STRING NOT NULL,
  regiondblivename STRING NOT NULL,
  regiondbcdcname STRING,
  regionactive INT64 NOT NULL,
  lastetlupdate DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  regionservername STRING,
  regionssrsstagepackage STRING,
  regionlastrunstagecompleteflag INT64,
  regionlastrunstagefailuremessage STRING,
  regionredirectlogpath STRING,
  regionprefix STRING,
  timezonedifference INT64,
  valescoindicator STRING,
  accountprefix STRING,
  PRIMARY KEY (regionkey) NOT ENFORCED
)
;