CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspepaytaxex`
AS SELECT
  `artiva_stgpspepaytaxex`.pspepaytxkey,
  `artiva_stgpspepaytaxex`.pspepaytxpayid,
  `artiva_stgpspepaytaxex`.pspepaytxtax,
  `artiva_stgpspepaytaxex`.pspepaytxtaxop,
  `artiva_stgpspepaytaxex`.pspepaytxtyp
  FROM
    edwpsc.`artiva_stgpspepaytaxex`
;