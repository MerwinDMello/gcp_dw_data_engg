CREATE OR REPLACE VIEW edwpsc_views.`pv_juncdiagnosiscodecoid`
AS SELECT
  `pv_juncdiagnosiscodecoid`.juncdiagnosiscodecoidkey,
  `pv_juncdiagnosiscodecoid`.diagnosiscodekey,
  `pv_juncdiagnosiscodecoid`.coid,
  `pv_juncdiagnosiscodecoid`.insertedby,
  `pv_juncdiagnosiscodecoid`.inserteddtm,
  `pv_juncdiagnosiscodecoid`.modifiedby,
  `pv_juncdiagnosiscodecoid`.modifieddtm,
  `pv_juncdiagnosiscodecoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_juncdiagnosiscodecoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_juncdiagnosiscodecoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;