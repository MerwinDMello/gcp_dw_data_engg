CREATE OR REPLACE VIEW edwpsc_views.`ecw_reportsecuritymasterdata`
AS SELECT
  `ecw_reportsecuritymasterdata`.user34,
  `ecw_reportsecuritymasterdata`.masterdatacontrol
  FROM
    edwpsc_base_views.`ecw_reportsecuritymasterdata`
;