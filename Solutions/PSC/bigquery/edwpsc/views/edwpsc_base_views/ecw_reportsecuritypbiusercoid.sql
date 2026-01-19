CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_reportsecuritypbiusercoid`
AS SELECT
  `ecw_reportsecuritypbiusercoid`.securecoidasuser34,
  `ecw_reportsecuritypbiusercoid`.coid,
  `ecw_reportsecuritypbiusercoid`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_reportsecuritypbiusercoid`
;