CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpsocroutesteps`
AS SELECT
  `artiva_stgpsocroutesteps`.psocrkey,
  `artiva_stgpsocroutesteps`.psocrname
  FROM
    edwpsc.`artiva_stgpsocroutesteps`
;