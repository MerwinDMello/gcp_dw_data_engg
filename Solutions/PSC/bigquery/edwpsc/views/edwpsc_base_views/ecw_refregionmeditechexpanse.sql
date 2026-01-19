CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refregionmeditechexpanse`
AS SELECT
  `ecw_refregionmeditechexpanse`.regionkey,
  `ecw_refregionmeditechexpanse`.regionname,
  `ecw_refregionmeditechexpanse`.regionsourceconnectionstring,
  `ecw_refregionmeditechexpanse`.regionsourceactive,
  `ecw_refregionmeditechexpanse`.runorder,
  `ecw_refregionmeditechexpanse`.regiondbname,
  `ecw_refregionmeditechexpanse`.regionservername,
  `ecw_refregionmeditechexpanse`.regionssrsstagepackage,
  `ecw_refregionmeditechexpanse`.regionlastrunstagecompleteflag,
  `ecw_refregionmeditechexpanse`.regionlastrunstagefailuremessage,
  `ecw_refregionmeditechexpanse`.regionredirectlogpath
  FROM
    edwpsc.`ecw_refregionmeditechexpanse`
;