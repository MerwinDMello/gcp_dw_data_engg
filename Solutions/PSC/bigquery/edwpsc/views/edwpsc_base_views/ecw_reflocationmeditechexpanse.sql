CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_reflocationmeditechexpanse`
AS SELECT
  `ecw_reflocationmeditechexpanse`.locationkey,
  `ecw_reflocationmeditechexpanse`.regionkey,
  `ecw_reflocationmeditechexpanse`.locationname,
  `ecw_reflocationmeditechexpanse`.locationtype,
  `ecw_reflocationmeditechexpanse`.siteid,
  `ecw_reflocationmeditechexpanse`.coid,
  `ecw_reflocationmeditechexpanse`.deleteflag,
  `ecw_reflocationmeditechexpanse`.sourceprimarykeyvalue,
  `ecw_reflocationmeditechexpanse`.sourcearecordlastupdated,
  `ecw_reflocationmeditechexpanse`.dwlastupdatedatetime,
  `ecw_reflocationmeditechexpanse`.sourcesystemcode,
  `ecw_reflocationmeditechexpanse`.insertedby,
  `ecw_reflocationmeditechexpanse`.inserteddtm,
  `ecw_reflocationmeditechexpanse`.modifiedby,
  `ecw_reflocationmeditechexpanse`.modifieddtm
  FROM
    edwpsc.`ecw_reflocationmeditechexpanse`
;