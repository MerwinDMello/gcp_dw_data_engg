CREATE OR REPLACE VIEW edwpsc_views.`ecw_refencounteractionlogtype`
AS SELECT
  `ecw_refencounteractionlogtype`.encounteractionlogtypekey,
  `ecw_refencounteractionlogtype`.encounteractionlogtypecode,
  `ecw_refencounteractionlogtype`.encounteractionlogtypename,
  `ecw_refencounteractionlogtype`.dwlastupdatedatetime,
  `ecw_refencounteractionlogtype`.sourcesystemcode,
  `ecw_refencounteractionlogtype`.insertedby,
  `ecw_refencounteractionlogtype`.inserteddtm,
  `ecw_refencounteractionlogtype`.modifiedby,
  `ecw_refencounteractionlogtype`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refencounteractionlogtype`
;