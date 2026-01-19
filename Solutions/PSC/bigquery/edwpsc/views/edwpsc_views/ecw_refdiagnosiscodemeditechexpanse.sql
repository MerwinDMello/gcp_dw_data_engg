CREATE OR REPLACE VIEW edwpsc_views.`ecw_refdiagnosiscodemeditechexpanse`
AS SELECT
  `ecw_refdiagnosiscodemeditechexpanse`.diagnosiscodekey,
  `ecw_refdiagnosiscodemeditechexpanse`.diagnosiscode,
  `ecw_refdiagnosiscodemeditechexpanse`.diagnosisname,
  `ecw_refdiagnosiscodemeditechexpanse`.deleteflag,
  `ecw_refdiagnosiscodemeditechexpanse`.sourceaprimarykeyvalue,
  `ecw_refdiagnosiscodemeditechexpanse`.sourcearecordlastupdated,
  `ecw_refdiagnosiscodemeditechexpanse`.dwlastupdatedatetime,
  `ecw_refdiagnosiscodemeditechexpanse`.sourcesystemcode,
  `ecw_refdiagnosiscodemeditechexpanse`.insertedby,
  `ecw_refdiagnosiscodemeditechexpanse`.inserteddtm,
  `ecw_refdiagnosiscodemeditechexpanse`.modifiedby,
  `ecw_refdiagnosiscodemeditechexpanse`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refdiagnosiscodemeditechexpanse`
;