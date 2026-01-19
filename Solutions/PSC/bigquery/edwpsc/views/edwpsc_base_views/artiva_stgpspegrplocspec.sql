CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspegrplocspec`
AS SELECT
  `artiva_stgpspegrplocspec`.pspeglsglid,
  `artiva_stgpspegrplocspec`.pspeglskey,
  `artiva_stgpspegrplocspec`.pspeglsspecname,
  `artiva_stgpspegrplocspec`.pspeglstaxonomy
  FROM
    edwpsc.`artiva_stgpspegrplocspec`
;