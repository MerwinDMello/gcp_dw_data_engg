CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpsholdrulesutility_pshrunotes`
AS SELECT
  `artiva_stgpsholdrulesutility_pshrunotes`.note_cnt,
  `artiva_stgpsholdrulesutility_pshrunotes`.note_date,
  `artiva_stgpsholdrulesutility_pshrunotes`.note_time,
  `artiva_stgpsholdrulesutility_pshrunotes`.note_type,
  `artiva_stgpsholdrulesutility_pshrunotes`.note_user,
  `artiva_stgpsholdrulesutility_pshrunotes`.pshrukey,
  `artiva_stgpsholdrulesutility_pshrunotes`.pshrunotes,
  `artiva_stgpsholdrulesutility_pshrunotes`.notedatetime,
  `artiva_stgpsholdrulesutility_pshrunotes`.insertedby,
  `artiva_stgpsholdrulesutility_pshrunotes`.inserteddtm,
  `artiva_stgpsholdrulesutility_pshrunotes`.modifiedby,
  `artiva_stgpsholdrulesutility_pshrunotes`.modifieddtm,
  `artiva_stgpsholdrulesutility_pshrunotes`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`artiva_stgpsholdrulesutility_pshrunotes`
;