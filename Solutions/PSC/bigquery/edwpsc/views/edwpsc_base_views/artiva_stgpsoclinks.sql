CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpsoclinks`
AS SELECT
  `artiva_stgpsoclinks`.psoclicount,
  `artiva_stgpsoclinks`.psoclidate,
  `artiva_stgpsoclinks`.psoclidesc,
  `artiva_stgpsoclinks`.psoclileadmsg,
  `artiva_stgpsoclinks`.psocliuser,
  `artiva_stgpsoclinks`.psocliuserchgdte,
  `artiva_stgpsoclinks`.psoclikey
  FROM
    edwpsc.`artiva_stgpsoclinks`
;