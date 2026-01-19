CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpsocactions`
AS SELECT
  `artiva_stgpsocactions`.psocakey,
  `artiva_stgpsocactions`.psocaname
  FROM
    edwpsc.`artiva_stgpsocactions`
;