CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refphynavigationurl`
AS SELECT
  `ecw_refphynavigationurl`.dwcrefphynavigationurlkey,
  `ecw_refphynavigationurl`.defaultaccess,
  `ecw_refphynavigationurl`.document_id,
  `ecw_refphynavigationurl`.project,
  `ecw_refphynavigationurl`.image_path,
  `ecw_refphynavigationurl`.url
  FROM
    edwpsc.`ecw_refphynavigationurl`
;