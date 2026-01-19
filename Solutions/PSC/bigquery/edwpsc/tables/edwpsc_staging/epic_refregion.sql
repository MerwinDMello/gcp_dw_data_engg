CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refregion
(
  regionkey INT64 NOT NULL,
  regionname STRING NOT NULL,
  regionsourceconnectionstring STRING,
  regionsourceactive INT64 NOT NULL,
  runorder INT64,
  regiondbname STRING,
  regionservername STRING,
  regionssrsstagepackage STRING,
  regionlastrunstagecompleteflag INT64,
  regionlastrunstagefailuremessage STRING,
  regionredirectlogpath STRING,
  timezonedifference INT64,
  PRIMARY KEY (regionkey) NOT ENFORCED
)
;