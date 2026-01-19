CREATE OR REPLACE VIEW edwpsc_views.`pv_refdiagnosiscode`
AS SELECT
  `pv_refdiagnosiscode`.diagnosiscodekey,
  `pv_refdiagnosiscode`.diagnosiscode,
  `pv_refdiagnosiscode`.diagnosisname,
  `pv_refdiagnosiscode`.sourceaprimarykeyvalue,
  `pv_refdiagnosiscode`.sourcearecordlastupdated,
  `pv_refdiagnosiscode`.sourcebprimarykeyvalue,
  `pv_refdiagnosiscode`.sourcebrecordlastupdated,
  `pv_refdiagnosiscode`.deleteflag,
  `pv_refdiagnosiscode`.dwlastupdatedatetime,
  `pv_refdiagnosiscode`.sourcesystemcode,
  `pv_refdiagnosiscode`.insertedby,
  `pv_refdiagnosiscode`.inserteddtm,
  `pv_refdiagnosiscode`.modifiedby,
  `pv_refdiagnosiscode`.modifieddtm
  FROM
    edwpsc_base_views.`pv_refdiagnosiscode`
;