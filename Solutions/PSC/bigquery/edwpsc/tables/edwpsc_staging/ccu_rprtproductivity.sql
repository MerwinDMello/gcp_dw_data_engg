CREATE TABLE IF NOT EXISTS edwpsc_staging.ccu_rprtproductivity
(
  coder34id STRING,
  touchdatekey DATE,
  eomonthdate DATE,
  sourcesystemcode STRING,
  notinventoryclaims INT64,
  notinventorycpts INT64,
  inventoryclaims INT64,
  inventorycpts INT64,
  statuschanges INT64,
  coidcount INT64,
  dwlastupdatedatetime DATE
)
;