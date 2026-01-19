CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspecpidpay`
AS SELECT
  `artiva_stgpspecpidpay`.pspecpkey,
  `artiva_stgpspecpidpay`.pspecpcpidid,
  `artiva_stgpspecpidpay`.pspecppayid
  FROM
    edwpsc.`artiva_stgpspecpidpay`
;