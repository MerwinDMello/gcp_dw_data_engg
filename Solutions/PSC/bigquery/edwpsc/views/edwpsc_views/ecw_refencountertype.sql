CREATE OR REPLACE VIEW edwpsc_views.`ecw_refencountertype`
AS SELECT
  `ecw_refencountertype`.encountertypekey,
  `ecw_refencountertype`.enctype,
  `ecw_refencountertype`.enctypedescription
  FROM
    edwpsc_base_views.`ecw_refencountertype`
;