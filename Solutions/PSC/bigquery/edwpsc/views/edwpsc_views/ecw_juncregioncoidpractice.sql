CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncregioncoidpractice`
AS SELECT
  `ecw_juncregioncoidpractice`.juncregioncoidpracticekey,
  `ecw_juncregioncoidpractice`.regionkey,
  `ecw_juncregioncoidpractice`.coid,
  `ecw_juncregioncoidpractice`.practicekey,
  `ecw_juncregioncoidpractice`.insertedby,
  `ecw_juncregioncoidpractice`.inserteddtm,
  `ecw_juncregioncoidpractice`.modifiedby,
  `ecw_juncregioncoidpractice`.modifieddtm,
  `ecw_juncregioncoidpractice`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_juncregioncoidpractice`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncregioncoidpractice`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;