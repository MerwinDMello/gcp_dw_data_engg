CREATE OR REPLACE VIEW edwpsc_views.`ecw_reportsecurityreport`
AS SELECT
  `ecw_reportsecurityreport`.reportid,
  `ecw_reportsecurityreport`.reportname
  FROM
    edwpsc_base_views.`ecw_reportsecurityreport`
;