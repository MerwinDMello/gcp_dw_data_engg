CREATE OR REPLACE VIEW edwpsc_views.`ecw_refiplanmeditechexpanse`
AS SELECT
  `ecw_refiplanmeditechexpanse`.iplankey,
  `ecw_refiplanmeditechexpanse`.regionkey,
  `ecw_refiplanmeditechexpanse`.iplanname,
  `ecw_refiplanmeditechexpanse`.iplanprimaryaddressline1,
  `ecw_refiplanmeditechexpanse`.iplanprimaryaddressline2,
  `ecw_refiplanmeditechexpanse`.iplanprimarygeographykey,
  `ecw_refiplanmeditechexpanse`.iplangroupid,
  `ecw_refiplanmeditechexpanse`.deleteflag,
  `ecw_refiplanmeditechexpanse`.sourceprimarykeyvalue,
  `ecw_refiplanmeditechexpanse`.sourcearecordlastupdated,
  `ecw_refiplanmeditechexpanse`.dwlastupdatedatetime,
  `ecw_refiplanmeditechexpanse`.sourcesystemcode,
  `ecw_refiplanmeditechexpanse`.insertedby,
  `ecw_refiplanmeditechexpanse`.inserteddtm,
  `ecw_refiplanmeditechexpanse`.modifiedby,
  `ecw_refiplanmeditechexpanse`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refiplanmeditechexpanse`
;