CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspeppieffdates`
AS SELECT
  `artiva_stgpspeppieffdates`.pspeppieffdte,
  `artiva_stgpspeppieffdates`.pspeppieffkey,
  `artiva_stgpspeppieffdates`.pspeppieffppiid,
  `artiva_stgpspeppieffdates`.pspeppieffprodid,
  `artiva_stgpspeppieffdates`.pspeppieffprovlocid
  FROM
    edwpsc.`artiva_stgpspeppieffdates`
;