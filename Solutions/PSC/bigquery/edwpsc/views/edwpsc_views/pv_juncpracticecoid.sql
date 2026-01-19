CREATE OR REPLACE VIEW edwpsc_views.`pv_juncpracticecoid`
AS SELECT
  `pv_juncpracticecoid`.juncpracticecoid,
  `pv_juncpracticecoid`.practicekey,
  `pv_juncpracticecoid`.coid,
  `pv_juncpracticecoid`.insertedby,
  `pv_juncpracticecoid`.inserteddtm,
  `pv_juncpracticecoid`.modifiedby,
  `pv_juncpracticecoid`.modifieddtm,
  `pv_juncpracticecoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_juncpracticecoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_juncpracticecoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;