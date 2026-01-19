CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refdiagnosiscode`
AS SELECT
  `ecw_refdiagnosiscode`.diagnosiscodekey,
  `ecw_refdiagnosiscode`.diagnosiscode,
  `ecw_refdiagnosiscode`.diagnosisname,
  `ecw_refdiagnosiscode`.sourceaprimarykeyvalue,
  `ecw_refdiagnosiscode`.sourcearecordlastupdated,
  `ecw_refdiagnosiscode`.sourcebprimarykeyvalue,
  `ecw_refdiagnosiscode`.sourcebrecordlastupdated,
  `ecw_refdiagnosiscode`.dwlastupdatedatetime,
  `ecw_refdiagnosiscode`.sourcesystemcode,
  `ecw_refdiagnosiscode`.insertedby,
  `ecw_refdiagnosiscode`.inserteddtm,
  `ecw_refdiagnosiscode`.modifiedby,
  `ecw_refdiagnosiscode`.modifieddtm,
  `ecw_refdiagnosiscode`.deleteflag
  FROM
    edwpsc.`ecw_refdiagnosiscode`
;