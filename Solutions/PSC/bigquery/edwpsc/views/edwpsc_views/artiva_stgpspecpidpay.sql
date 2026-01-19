CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspecpidpay`
AS SELECT
  `artiva_stgpspecpidpay`.pspecpkey,
  `artiva_stgpspecpidpay`.pspecpcpidid,
  `artiva_stgpspecpidpay`.pspecppayid
  FROM
    edwpsc_base_views.`artiva_stgpspecpidpay`
;