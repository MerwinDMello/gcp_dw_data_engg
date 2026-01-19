CREATE OR REPLACE VIEW edwpsc_base_views.`pv_stgerafilepayer`
AS SELECT
  `pv_stgerafilepayer`.payer_id,
  `pv_stgerafilepayer`.fileid,
  `pv_stgerafilepayer`.bpr_id,
  `pv_stgerafilepayer`.trn_id,
  `pv_stgerafilepayer`.payer01,
  `pv_stgerafilepayer`.payer02,
  `pv_stgerafilepayer`.payer03,
  `pv_stgerafilepayer`.payer04,
  `pv_stgerafilepayer`.payer05,
  `pv_stgerafilepayer`.payer06,
  `pv_stgerafilepayer`.payer07,
  `pv_stgerafilepayer`.payer08,
  `pv_stgerafilepayer`.payer09,
  `pv_stgerafilepayer`.payer10,
  `pv_stgerafilepayer`.payersegment,
  `pv_stgerafilepayer`.inserteddtm,
  `pv_stgerafilepayer`.gs_id,
  `pv_stgerafilepayer`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_stgerafilepayer`
;