CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspesysalerts_pspesysalrnotes`
AS SELECT
  `artiva_stgpspesysalerts_pspesysalrnotes`.note_cnt,
  `artiva_stgpspesysalerts_pspesysalrnotes`.note_date,
  `artiva_stgpspesysalerts_pspesysalrnotes`.note_time,
  `artiva_stgpspesysalerts_pspesysalrnotes`.note_type,
  `artiva_stgpspesysalerts_pspesysalrnotes`.note_user,
  `artiva_stgpspesysalerts_pspesysalrnotes`.pspesysalrkey,
  `artiva_stgpspesysalerts_pspesysalrnotes`.pspesysalrnotes
  FROM
    edwpsc_base_views.`artiva_stgpspesysalerts_pspesysalrnotes`
;