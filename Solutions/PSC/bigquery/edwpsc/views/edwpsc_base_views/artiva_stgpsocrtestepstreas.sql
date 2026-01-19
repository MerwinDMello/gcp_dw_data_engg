CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpsocrtestepstreas`
AS SELECT
  `artiva_stgpsocrtestepstreas`.psocrkey,
  `artiva_stgpsocrtestepstreas`.psocrname
  FROM
    edwpsc.`artiva_stgpsocrtestepstreas`
;