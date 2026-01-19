CREATE TABLE IF NOT EXISTS edwpsc.ecw_reportsecurity
(
  user34 STRING,
  securitycoid STRING,
  securityrole STRING,
  mstrrole STRING,
  mstraltcoid STRING,
  mstrcoid STRING,
  mstrmarket STRING,
  mstrdivision STRING,
  mstrgroup STRING,
  mstrfullcoid STRING,
  dashboardsecuritycoid STRING,
  dashboardsecurityrole STRING,
  missingcoidsfrommstr STRING,
  ssrsonlyflag INT64 NOT NULL,
  ssrslastaccessed DATETIME,
  dwlastupdatedatetime DATETIME
)
;