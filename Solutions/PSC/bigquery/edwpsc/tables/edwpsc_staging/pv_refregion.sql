CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_refregion
(
  regionkey INT64 NOT NULL,
  regionname STRING NOT NULL,
  regionsourceconnectionstring STRING NOT NULL,
  regionsourceactive INT64 NOT NULL,
  artivaregionkey INT64,
  regiondbname STRING,
  regionservername STRING,
  regionssrsstagepackage STRING,
  regionlastrunstagecompleteflag INT64,
  regionlastrunstagefailuremessage STRING,
  regionredirectlogpath STRING,
  PRIMARY KEY (regionkey) NOT ENFORCED
)
;