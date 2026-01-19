CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncpracticecoid`
AS SELECT
  `ecw_juncpracticecoid`.juncpracticecoid,
  `ecw_juncpracticecoid`.practicekey,
  `ecw_juncpracticecoid`.coid,
  `ecw_juncpracticecoid`.insertedby,
  `ecw_juncpracticecoid`.inserteddtm,
  `ecw_juncpracticecoid`.modifiedby,
  `ecw_juncpracticecoid`.modifieddtm,
  `ecw_juncpracticecoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_juncpracticecoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncpracticecoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;