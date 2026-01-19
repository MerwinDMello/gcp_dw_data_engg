CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspecpids_pspecpidnotes`
AS SELECT
  `artiva_stgpspecpids_pspecpidnotes`.note_cnt,
  `artiva_stgpspecpids_pspecpidnotes`.note_date,
  `artiva_stgpspecpids_pspecpidnotes`.note_time,
  `artiva_stgpspecpids_pspecpidnotes`.note_type,
  `artiva_stgpspecpids_pspecpidnotes`.note_user,
  `artiva_stgpspecpids_pspecpidnotes`.pspecpidkey,
  `artiva_stgpspecpids_pspecpidnotes`.pspecpidnotes
  FROM
    edwpsc.`artiva_stgpspecpids_pspecpidnotes`
;