CREATE OR REPLACE VIEW edwpsc_views.`ecw_reportsecurityrole`
AS SELECT
  `ecw_reportsecurityrole`.roleid,
  `ecw_reportsecurityrole`.rolename
  FROM
    edwpsc_base_views.`ecw_reportsecurityrole`
;