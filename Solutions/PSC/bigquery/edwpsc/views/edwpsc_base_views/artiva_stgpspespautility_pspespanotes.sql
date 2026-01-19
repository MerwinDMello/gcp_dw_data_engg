CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspespautility_pspespanotes`
AS SELECT
  `artiva_stgpspespautility_pspespanotes`.note_cnt,
  `artiva_stgpspespautility_pspespanotes`.note_date,
  `artiva_stgpspespautility_pspespanotes`.note_time,
  `artiva_stgpspespautility_pspespanotes`.notedatetime,
  `artiva_stgpspespautility_pspespanotes`.note_type,
  `artiva_stgpspespautility_pspespanotes`.note_user,
  `artiva_stgpspespautility_pspespanotes`.pspespakey,
  `artiva_stgpspespautility_pspespanotes`.pspespanotes,
  `artiva_stgpspespautility_pspespanotes`.insertedby,
  `artiva_stgpspespautility_pspespanotes`.inserteddtm,
  `artiva_stgpspespautility_pspespanotes`.modifiedby,
  `artiva_stgpspespautility_pspespanotes`.modifieddtm,
  `artiva_stgpspespautility_pspespanotes`.dwlastupdatedatetime
  FROM
    edwpsc.`artiva_stgpspespautility_pspespanotes`
;