CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncdiagnosiscodecoid`
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
    edwpsc.`ecw_juncdiagnosiscodecoid`
;