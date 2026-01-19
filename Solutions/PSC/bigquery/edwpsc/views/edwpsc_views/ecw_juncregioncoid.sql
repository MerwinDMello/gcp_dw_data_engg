CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncregioncoid`
AS SELECT
  `ecw_juncregioncoid`.juncregioncoidkey,
  `ecw_juncregioncoid`.regionkey,
  `ecw_juncregioncoid`.coid,
  `ecw_juncregioncoid`.insertedby,
  `ecw_juncregioncoid`.inserteddtm,
  `ecw_juncregioncoid`.modifiedby,
  `ecw_juncregioncoid`.modifieddtm,
  `ecw_juncregioncoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_juncregioncoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncregioncoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;