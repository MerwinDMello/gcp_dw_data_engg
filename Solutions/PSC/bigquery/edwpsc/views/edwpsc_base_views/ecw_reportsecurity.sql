CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_reportsecurity`
AS SELECT
  `ecw_reportsecurity`.user34,
  `ecw_reportsecurity`.securitycoid,
  `ecw_reportsecurity`.securityrole,
  `ecw_reportsecurity`.mstrrole,
  `ecw_reportsecurity`.mstraltcoid,
  `ecw_reportsecurity`.mstrcoid,
  `ecw_reportsecurity`.mstrmarket,
  `ecw_reportsecurity`.mstrdivision,
  `ecw_reportsecurity`.mstrgroup,
  `ecw_reportsecurity`.mstrfullcoid,
  `ecw_reportsecurity`.dashboardsecuritycoid,
  `ecw_reportsecurity`.dashboardsecurityrole,
  `ecw_reportsecurity`.missingcoidsfrommstr,
  `ecw_reportsecurity`.ssrsonlyflag,
  `ecw_reportsecurity`.ssrslastaccessed,
  `ecw_reportsecurity`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_reportsecurity`
;