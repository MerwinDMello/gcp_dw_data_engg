CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_reportsecurityreporttorole`
AS SELECT
  `ecw_reportsecurityreporttorole`.reportroleid,
  `ecw_reportsecurityreporttorole`.roleid,
  `ecw_reportsecurityreporttorole`.reportid
  FROM
    edwpsc.`ecw_reportsecurityreporttorole`
;