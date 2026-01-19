CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspepaypends`
AS SELECT
  `artiva_stgpspepaypends`.pspepaypdkey,
  `artiva_stgpspepaypends`.pspepaypdpayid,
  `artiva_stgpspepaypends`.pspepaypdrefpayid
  FROM
    edwpsc.`artiva_stgpspepaypends`
;