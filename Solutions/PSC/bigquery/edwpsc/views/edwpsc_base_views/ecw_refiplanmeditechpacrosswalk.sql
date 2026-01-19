CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refiplanmeditechpacrosswalk`
AS SELECT
  `ecw_refiplanmeditechpacrosswalk`.meditechpacrosswalkkey,
  `ecw_refiplanmeditechpacrosswalk`.sscstate,
  `ecw_refiplanmeditechpacrosswalk`.sscname,
  `ecw_refiplanmeditechpacrosswalk`.hospitalcoid,
  `ecw_refiplanmeditechpacrosswalk`.meditechiplanid,
  `ecw_refiplanmeditechpacrosswalk`.paiplanid,
  `ecw_refiplanmeditechpacrosswalk`.paiplanname,
  `ecw_refiplanmeditechpacrosswalk`.paiplanfinancialclasskey,
  `ecw_refiplanmeditechpacrosswalk`.painsuranceactiveflag,
  `ecw_refiplanmeditechpacrosswalk`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_refiplanmeditechpacrosswalk`
;