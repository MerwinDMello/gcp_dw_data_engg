CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_rprtbillingcoodinatorproductivity
(
  weekstartdate DATE,
  weekenddate DATE,
  savedby STRING,
  actiondate DATE,
  month DATE,
  nexttouchtoccu STRING,
  nexttouchtooutbox STRING,
  claimsubstatusname STRING,
  cocid STRING,
  touchcount INT64,
  sourcesystem STRING,
  inserteddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  billingcoordinatorprodkey INT64 NOT NULL
)
;