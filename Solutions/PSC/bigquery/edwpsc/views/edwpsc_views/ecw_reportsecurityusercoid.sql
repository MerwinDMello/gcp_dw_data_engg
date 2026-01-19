CREATE OR REPLACE VIEW edwpsc_views.`ecw_reportsecurityusercoid`
AS SELECT
  `ecw_reportsecurityusercoid`.fulluser34,
  `ecw_reportsecurityusercoid`.userdomain,
  `ecw_reportsecurityusercoid`.user34,
  `ecw_reportsecurityusercoid`.userpbi,
  `ecw_reportsecurityusercoid`.coid,
  `ecw_reportsecurityusercoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_reportsecurityusercoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_reportsecurityusercoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;