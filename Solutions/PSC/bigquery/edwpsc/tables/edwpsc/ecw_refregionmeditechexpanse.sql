CREATE TABLE IF NOT EXISTS edwpsc.ecw_refregionmeditechexpanse
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
  PRIMARY KEY (regionkey) NOT ENFORCED
)
;