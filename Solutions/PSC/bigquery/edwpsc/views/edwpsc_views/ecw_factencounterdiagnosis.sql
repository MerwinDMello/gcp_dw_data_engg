CREATE OR REPLACE VIEW edwpsc_views.`ecw_factencounterdiagnosis`
AS SELECT
  `ecw_factencounterdiagnosis`.encounterdiagnosiskey,
  `ecw_factencounterdiagnosis`.regionkey,
  `ecw_factencounterdiagnosis`.coid,
  `ecw_factencounterdiagnosis`.encounterkey,
  `ecw_factencounterdiagnosis`.encounterid,
  `ecw_factencounterdiagnosis`.diagnosiscodekey,
  `ecw_factencounterdiagnosis`.icdcode,
  `ecw_factencounterdiagnosis`.icdorder,
  `ecw_factencounterdiagnosis`.icdtype,
  `ecw_factencounterdiagnosis`.icddescription,
  `ecw_factencounterdiagnosis`.visitdate,
  `ecw_factencounterdiagnosis`.primaryflag,
  `ecw_factencounterdiagnosis`.deleteflag,
  `ecw_factencounterdiagnosis`.specify,
  `ecw_factencounterdiagnosis`.notes,
  `ecw_factencounterdiagnosis`.assessmentonsetdate,
  `ecw_factencounterdiagnosis`.snomed,
  `ecw_factencounterdiagnosis`.snomeddescription,
  `ecw_factencounterdiagnosis`.synonymid,
  `ecw_factencounterdiagnosis`.axis,
  `ecw_factencounterdiagnosis`.sourceprimarykeyvalue,
  `ecw_factencounterdiagnosis`.sourcerecordlastupdated,
  `ecw_factencounterdiagnosis`.dwlastupdatedatetime,
  `ecw_factencounterdiagnosis`.sourcesystemcode,
  `ecw_factencounterdiagnosis`.insertedby,
  `ecw_factencounterdiagnosis`.inserteddtm,
  `ecw_factencounterdiagnosis`.modifiedby,
  `ecw_factencounterdiagnosis`.modifieddtm,
  `ecw_factencounterdiagnosis`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_factencounterdiagnosis`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factencounterdiagnosis`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;