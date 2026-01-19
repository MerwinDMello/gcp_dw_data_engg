CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refstate`
AS SELECT
  `ecw_refstate`.statekey,
  `ecw_refstate`.statename,
  `ecw_refstate`.statecapitalcity,
  `ecw_refstate`.dwlastupdatedatetime,
  `ecw_refstate`.sourcesystemcode,
  `ecw_refstate`.insertedby,
  `ecw_refstate`.inserteddtm,
  `ecw_refstate`.modifiedby,
  `ecw_refstate`.modifieddtm
  FROM
    edwpsc.`ecw_refstate`
;