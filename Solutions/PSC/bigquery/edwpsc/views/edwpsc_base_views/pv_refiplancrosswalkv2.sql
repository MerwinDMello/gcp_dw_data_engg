CREATE OR REPLACE VIEW edwpsc_base_views.`pv_refiplancrosswalkv2`
AS SELECT
  `pv_refiplancrosswalkv2`.pvclass,
  `pv_refiplancrosswalkv2`.pvtype,
  `pv_refiplancrosswalkv2`.financialclasskey,
  `pv_refiplancrosswalkv2`.iplangroupname
  FROM
    edwpsc.`pv_refiplancrosswalkv2`
;