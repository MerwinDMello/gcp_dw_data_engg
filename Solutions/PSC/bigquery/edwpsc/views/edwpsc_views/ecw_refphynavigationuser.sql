CREATE OR REPLACE VIEW edwpsc_views.`ecw_refphynavigationuser`
AS SELECT
  `ecw_refphynavigationuser`.refphynavigationuserkey,
  `ecw_refphynavigationuser`.user_id,
  `ecw_refphynavigationuser`.lastname,
  `ecw_refphynavigationuser`.firstname,
  `ecw_refphynavigationuser`.role,
  `ecw_refphynavigationuser`.defaultaccess,
  `ecw_refphynavigationuser`.security_filter,
  `ecw_refphynavigationuser`.device
  FROM
    edwpsc_base_views.`ecw_refphynavigationuser`
;