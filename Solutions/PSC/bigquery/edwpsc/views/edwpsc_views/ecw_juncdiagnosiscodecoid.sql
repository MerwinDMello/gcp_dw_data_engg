CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncdiagnosiscodecoid`
AS SELECT
  `ecw_juncdiagnosiscodecoid`.juncdiagnosiscodecoidkey,
  `ecw_juncdiagnosiscodecoid`.diagnosiscodekey,
  `ecw_juncdiagnosiscodecoid`.coid,
  `ecw_juncdiagnosiscodecoid`.insertedby,
  `ecw_juncdiagnosiscodecoid`.inserteddtm,
  `ecw_juncdiagnosiscodecoid`.modifiedby,
  `ecw_juncdiagnosiscodecoid`.modifieddtm,
  `ecw_juncdiagnosiscodecoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_juncdiagnosiscodecoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncdiagnosiscodecoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;