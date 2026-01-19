CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refsourcesystem`
AS SELECT
  `ecw_refsourcesystem`.sourcesystemkey,
  `ecw_refsourcesystem`.sourcesystemcode,
  `ecw_refsourcesystem`.sourcesystemshortname,
  `ecw_refsourcesystem`.sourcesystemlongname,
  `ecw_refsourcesystem`.insertedby,
  `ecw_refsourcesystem`.inserteddtm,
  `ecw_refsourcesystem`.modifiedby,
  `ecw_refsourcesystem`.modifieddtm,
  `ecw_refsourcesystem`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_refsourcesystem`
;