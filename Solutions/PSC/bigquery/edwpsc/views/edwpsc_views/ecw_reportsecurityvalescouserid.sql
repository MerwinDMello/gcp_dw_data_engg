CREATE OR REPLACE VIEW edwpsc_views.`ecw_reportsecurityvalescouserid`
AS SELECT
  `ecw_reportsecurityvalescouserid`.user34,
  `ecw_reportsecurityvalescouserid`.roleid,
  `ecw_reportsecurityvalescouserid`.usertypename,
  `ecw_reportsecurityvalescouserid`.securitycoid
  FROM
    edwpsc_base_views.`ecw_reportsecurityvalescouserid`
;