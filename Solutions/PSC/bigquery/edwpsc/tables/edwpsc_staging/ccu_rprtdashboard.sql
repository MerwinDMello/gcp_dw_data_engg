CREATE TABLE IF NOT EXISTS edwpsc_staging.ccu_rprtdashboard
(
  ccudashboardkey INT64 NOT NULL,
  dashboardreportname STRING,
  coid STRING,
  owner STRING,
  eomdate DATE,
  eomdateyyyymm STRING,
  category1 STRING,
  category2 STRING,
  metriccategory STRING,
  mtdcount INT64,
  totalmonthcount INT64,
  resolveddaysbysubmitdate INT64,
  resolveddaysbyassigneemodifieddate INT64,
  loaddate DATE,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  sourcetablelastloaddate DATE,
  providercountactive NUMERIC(35, 6)
)
;