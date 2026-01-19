CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factencounterdiagnosismeditechexpanse`
AS SELECT
  `ecw_factencounterdiagnosismeditechexpanse`.encounterdiagnosismtxkey,
  `ecw_factencounterdiagnosismeditechexpanse`.regionkey,
  `ecw_factencounterdiagnosismeditechexpanse`.encounterkey,
  `ecw_factencounterdiagnosismeditechexpanse`.diagnosiscodekey,
  `ecw_factencounterdiagnosismeditechexpanse`.diagnosiscode,
  `ecw_factencounterdiagnosismeditechexpanse`.diagnosistransnum,
  `ecw_factencounterdiagnosismeditechexpanse`.diagnosisorder,
  `ecw_factencounterdiagnosismeditechexpanse`.diagnosisbchid,
  `ecw_factencounterdiagnosismeditechexpanse`.deleteflag,
  `ecw_factencounterdiagnosismeditechexpanse`.sourcelastupdateddate,
  `ecw_factencounterdiagnosismeditechexpanse`.dwlastupdatedatetime,
  `ecw_factencounterdiagnosismeditechexpanse`.sourcesystemcode,
  `ecw_factencounterdiagnosismeditechexpanse`.insertedby,
  `ecw_factencounterdiagnosismeditechexpanse`.inserteddtm,
  `ecw_factencounterdiagnosismeditechexpanse`.modifiedby,
  `ecw_factencounterdiagnosismeditechexpanse`.modifieddtm
  FROM
    edwpsc.`ecw_factencounterdiagnosismeditechexpanse`
;