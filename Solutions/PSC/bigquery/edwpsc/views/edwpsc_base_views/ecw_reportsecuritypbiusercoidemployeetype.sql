CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_reportsecuritypbiusercoidemployeetype`
AS SELECT
  `ecw_reportsecuritypbiusercoidemployeetype`.secureemployeetypeasuser34,
  `ecw_reportsecuritypbiusercoidemployeetype`.coid_et,
  `ecw_reportsecuritypbiusercoidemployeetype`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_reportsecuritypbiusercoidemployeetype`
;