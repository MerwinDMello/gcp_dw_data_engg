CREATE OR REPLACE VIEW edwpsc_base_views.`pv_juncdiagnosiscodecoid`
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
    edwpsc.`pv_juncdiagnosiscodecoid`
;