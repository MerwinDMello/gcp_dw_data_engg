CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspeprspecialty`
AS SELECT
  `artiva_stgpspeprspecialty`.pspeprspactiveind,
  `artiva_stgpspeprspecialty`.pspeprspgafid,
  `artiva_stgpspeprspecialty`.pspeprspkey,
  `artiva_stgpspeprspecialty`.pspeprspname,
  `artiva_stgpspeprspecialty`.pspeprspperfid,
  `artiva_stgpspeprspecialty`.pspeprspstatus,
  `artiva_stgpspeprspecialty`.pspeprsptaxonomy,
  `artiva_stgpspeprspecialty`.pspeprsptype
  FROM
    edwpsc.`artiva_stgpspeprspecialty`
;