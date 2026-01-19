CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspepayprod`
AS SELECT
  `artiva_stgpspepayprod`.pspepayprdesc,
  `artiva_stgpspepayprod`.pspepayprfinclass,
  `artiva_stgpspepayprod`.pspepayprkey,
  `artiva_stgpspepayprod`.pspepayprname,
  `artiva_stgpspepayprod`.pspepayprpayid
  FROM
    edwpsc.`artiva_stgpspepayprod`
;