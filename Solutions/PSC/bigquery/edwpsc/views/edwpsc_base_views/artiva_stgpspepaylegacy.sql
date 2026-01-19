CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspepaylegacy`
AS SELECT
  `artiva_stgpspepaylegacy`.pspepaylpkey,
  `artiva_stgpspepaylegacy`.pspepaylppayid,
  `artiva_stgpspepaylegacy`.pspepaylprefpayid
  FROM
    edwpsc.`artiva_stgpspepaylegacy`
;