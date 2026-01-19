CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refgeographymeditechexpanse`
AS SELECT
  `ecw_refgeographymeditechexpanse`.geographykey,
  `ecw_refgeographymeditechexpanse`.geographycity,
  `ecw_refgeographymeditechexpanse`.statekey,
  `ecw_refgeographymeditechexpanse`.geographyzipcode,
  `ecw_refgeographymeditechexpanse`.dwlastupdatedatetime,
  `ecw_refgeographymeditechexpanse`.sourcesystemcode,
  `ecw_refgeographymeditechexpanse`.insertedby,
  `ecw_refgeographymeditechexpanse`.inserteddtm,
  `ecw_refgeographymeditechexpanse`.modifiedby,
  `ecw_refgeographymeditechexpanse`.modifieddtm
  FROM
    edwpsc.`ecw_refgeographymeditechexpanse`
;