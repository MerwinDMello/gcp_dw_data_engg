CREATE OR REPLACE VIEW edwpsc_views.`ecw_refphynavigation`
AS SELECT
  `ecw_refphynavigation`.user_id,
  `ecw_refphynavigation`.lastname,
  `ecw_refphynavigation`.firstname,
  `ecw_refphynavigation`.role,
  `ecw_refphynavigation`.defaultaccess,
  `ecw_refphynavigation`.security_filter,
  `ecw_refphynavigation`.device,
  `ecw_refphynavigation`.project,
  `ecw_refphynavigation`.document_id,
  `ecw_refphynavigation`.image_path,
  `ecw_refphynavigation`.url
  FROM
    edwpsc_base_views.`ecw_refphynavigation`
;