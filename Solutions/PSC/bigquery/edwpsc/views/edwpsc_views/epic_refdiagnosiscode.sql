CREATE OR REPLACE VIEW edwpsc_views.`epic_refdiagnosiscode`
AS SELECT
  `epic_refdiagnosiscode`.diagnosiscodekey,
  `epic_refdiagnosiscode`.diagnosiscodename,
  `epic_refdiagnosiscode`.diagnosiscode,
  `epic_refdiagnosiscode`.diagnosisicd10,
  `epic_refdiagnosiscode`.dxid,
  `epic_refdiagnosiscode`.diagnosisexternalid,
  `epic_refdiagnosiscode`.deleteflag,
  `epic_refdiagnosiscode`.regionkey,
  `epic_refdiagnosiscode`.sourceaprimarykey,
  `epic_refdiagnosiscode`.sourceaprimarykeylastupdated,
  `epic_refdiagnosiscode`.dwlastupdatedatetime,
  `epic_refdiagnosiscode`.sourcesystemcode,
  `epic_refdiagnosiscode`.insertedby,
  `epic_refdiagnosiscode`.inserteddtm,
  `epic_refdiagnosiscode`.modifiedby,
  `epic_refdiagnosiscode`.modifieddtm
  FROM
    edwpsc_base_views.`epic_refdiagnosiscode`
;