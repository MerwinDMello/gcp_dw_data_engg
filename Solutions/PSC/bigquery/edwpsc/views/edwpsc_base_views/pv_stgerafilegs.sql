CREATE OR REPLACE VIEW edwpsc_base_views.`pv_stgerafilegs`
AS SELECT
  `pv_stgerafilegs`.gs_id,
  `pv_stgerafilegs`.fileid,
  `pv_stgerafilegs`.gs01,
  `pv_stgerafilegs`.gs02,
  `pv_stgerafilegs`.gs03,
  `pv_stgerafilegs`.gs04,
  `pv_stgerafilegs`.gs05,
  `pv_stgerafilegs`.gs06,
  `pv_stgerafilegs`.gs07,
  `pv_stgerafilegs`.gs08,
  `pv_stgerafilegs`.gssegment,
  `pv_stgerafilegs`.inserteddtm,
  `pv_stgerafilegs`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_stgerafilegs`
;