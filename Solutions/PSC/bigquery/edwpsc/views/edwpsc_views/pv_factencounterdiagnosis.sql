CREATE OR REPLACE VIEW edwpsc_views.`pv_factencounterdiagnosis`
AS SELECT
  `pv_factencounterdiagnosis`.encounterdiagnosiskey,
  `pv_factencounterdiagnosis`.regionkey,
  `pv_factencounterdiagnosis`.coid,
  `pv_factencounterdiagnosis`.practicename,
  `pv_factencounterdiagnosis`.encounterkey,
  `pv_factencounterdiagnosis`.encounterid,
  `pv_factencounterdiagnosis`.diagnosiscodekey,
  `pv_factencounterdiagnosis`.icd10,
  `pv_factencounterdiagnosis`.icd9,
  `pv_factencounterdiagnosis`.icdorder,
  `pv_factencounterdiagnosis`.visitdate,
  `pv_factencounterdiagnosis`.deleteflag,
  `pv_factencounterdiagnosis`.sourceprimarykeyvalue,
  `pv_factencounterdiagnosis`.sourcerecordlastupdated,
  `pv_factencounterdiagnosis`.dwlastupdatedatetime,
  `pv_factencounterdiagnosis`.sourcesystemcode,
  `pv_factencounterdiagnosis`.insertedby,
  `pv_factencounterdiagnosis`.inserteddtm,
  `pv_factencounterdiagnosis`.modifiedby,
  `pv_factencounterdiagnosis`.modifieddtm
  FROM
    edwpsc_base_views.`pv_factencounterdiagnosis`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factencounterdiagnosis`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;