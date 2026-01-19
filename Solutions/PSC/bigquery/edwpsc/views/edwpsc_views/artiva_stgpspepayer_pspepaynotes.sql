CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspepayer_pspepaynotes`
AS SELECT
  `artiva_stgpspepayer_pspepaynotes`.note_cnt,
  `artiva_stgpspepayer_pspepaynotes`.note_date,
  `artiva_stgpspepayer_pspepaynotes`.note_time,
  `artiva_stgpspepayer_pspepaynotes`.note_type,
  `artiva_stgpspepayer_pspepaynotes`.note_user,
  `artiva_stgpspepayer_pspepaynotes`.pspepaykey,
  `artiva_stgpspepayer_pspepaynotes`.pspepaynotes
  FROM
    edwpsc_base_views.`artiva_stgpspepayer_pspepaynotes`
;