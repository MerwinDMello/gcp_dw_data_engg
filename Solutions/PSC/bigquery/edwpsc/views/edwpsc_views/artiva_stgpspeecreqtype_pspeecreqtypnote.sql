CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeecreqtype_pspeecreqtypnote`
AS SELECT
  `artiva_stgpspeecreqtype_pspeecreqtypnote`.note_cnt,
  `artiva_stgpspeecreqtype_pspeecreqtypnote`.note_date,
  `artiva_stgpspeecreqtype_pspeecreqtypnote`.note_time,
  `artiva_stgpspeecreqtype_pspeecreqtypnote`.note_type,
  `artiva_stgpspeecreqtype_pspeecreqtypnote`.note_user,
  `artiva_stgpspeecreqtype_pspeecreqtypnote`.pspeecreqtypnote,
  `artiva_stgpspeecreqtype_pspeecreqtypnote`.pspeecrtid
  FROM
    edwpsc_base_views.`artiva_stgpspeecreqtype_pspeecreqtypnote`
;