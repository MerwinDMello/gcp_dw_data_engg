CREATE OR REPLACE VIEW edwpsc_views.`ecw_refiplanmeditechecwcrosswalk`
AS SELECT
  `ecw_refiplanmeditechecwcrosswalk`.meditechecwcrosswalkkey,
  `ecw_refiplanmeditechecwcrosswalk`.meditechiplanid,
  `ecw_refiplanmeditechecwcrosswalk`.ecwiplanname,
  `ecw_refiplanmeditechecwcrosswalk`.ecwiplanid,
  `ecw_refiplanmeditechecwcrosswalk`.isgovernmentflag,
  `ecw_refiplanmeditechecwcrosswalk`.isavailableinecwflag,
  `ecw_refiplanmeditechecwcrosswalk`.isbillablebymidlevelflag,
  `ecw_refiplanmeditechecwcrosswalk`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_refiplanmeditechecwcrosswalk`
;