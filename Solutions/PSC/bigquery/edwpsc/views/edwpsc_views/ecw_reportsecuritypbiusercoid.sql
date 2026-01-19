CREATE OR REPLACE VIEW edwpsc_views.`ecw_reportsecuritypbiusercoid`
AS SELECT
  `ecw_reportsecuritypbiusercoid`.securecoidasuser34,
  `ecw_reportsecuritypbiusercoid`.coid,
  `ecw_reportsecuritypbiusercoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_reportsecuritypbiusercoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_reportsecuritypbiusercoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;