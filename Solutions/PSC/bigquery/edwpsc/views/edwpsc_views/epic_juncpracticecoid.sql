CREATE OR REPLACE VIEW edwpsc_views.`epic_juncpracticecoid`
AS SELECT
  `epic_juncpracticecoid`.juncpracticecoid,
  `epic_juncpracticecoid`.practicekey,
  `epic_juncpracticecoid`.coid,
  `epic_juncpracticecoid`.insertedby,
  `epic_juncpracticecoid`.inserteddtm,
  `epic_juncpracticecoid`.modifiedby,
  `epic_juncpracticecoid`.modifieddtm,
  `epic_juncpracticecoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_juncpracticecoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_juncpracticecoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;