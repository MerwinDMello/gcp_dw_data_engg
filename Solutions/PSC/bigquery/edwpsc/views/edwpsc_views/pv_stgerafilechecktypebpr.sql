CREATE OR REPLACE VIEW edwpsc_views.`pv_stgerafilechecktypebpr`
AS SELECT
  `pv_stgerafilechecktypebpr`.bpr_id,
  `pv_stgerafilechecktypebpr`.fileid,
  `pv_stgerafilechecktypebpr`.bpr01,
  `pv_stgerafilechecktypebpr`.bpr02,
  `pv_stgerafilechecktypebpr`.bpr03,
  `pv_stgerafilechecktypebpr`.bpr04,
  `pv_stgerafilechecktypebpr`.bpr05,
  `pv_stgerafilechecktypebpr`.bpr06,
  `pv_stgerafilechecktypebpr`.bpr07,
  `pv_stgerafilechecktypebpr`.bpr08,
  `pv_stgerafilechecktypebpr`.bpr09,
  `pv_stgerafilechecktypebpr`.bpr10,
  `pv_stgerafilechecktypebpr`.bpr11,
  `pv_stgerafilechecktypebpr`.bpr12,
  `pv_stgerafilechecktypebpr`.bpr13,
  `pv_stgerafilechecktypebpr`.bpr14,
  `pv_stgerafilechecktypebpr`.bpr15,
  `pv_stgerafilechecktypebpr`.bpr16,
  `pv_stgerafilechecktypebpr`.bprsegment,
  `pv_stgerafilechecktypebpr`.inserteddtm,
  `pv_stgerafilechecktypebpr`.gs_id,
  `pv_stgerafilechecktypebpr`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_stgerafilechecktypebpr`
;