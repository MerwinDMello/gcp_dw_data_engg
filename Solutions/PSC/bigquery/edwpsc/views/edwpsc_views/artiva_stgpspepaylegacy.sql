CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspepaylegacy`
AS SELECT
  `artiva_stgpspepaylegacy`.pspepaylpkey,
  `artiva_stgpspepaylegacy`.pspepaylppayid,
  `artiva_stgpspepaylegacy`.pspepaylprefpayid
  FROM
    edwpsc_base_views.`artiva_stgpspepaylegacy`
;