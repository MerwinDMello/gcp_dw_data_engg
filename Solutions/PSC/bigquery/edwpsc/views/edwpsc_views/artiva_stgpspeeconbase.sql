CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeeconbase`
AS SELECT
  `artiva_stgpspeeconbase`.pspeecobdate,
  `artiva_stgpspeeconbase`.pspeecobdesc,
  `artiva_stgpspeeconbase`.pspeecobectid,
  `artiva_stgpspeeconbase`.pspeecobexportdte,
  `artiva_stgpspeeconbase`.pspeecobhandle,
  `artiva_stgpspeeconbase`.pspeecobid,
  `artiva_stgpspeeconbase`.pspeecobnew,
  `artiva_stgpspeeconbase`.pspeecobportalectid,
  `artiva_stgpspeeconbase`.pspeecobportalid,
  `artiva_stgpspeeconbase`.pspeecobportalkey,
  `artiva_stgpspeeconbase`.pspeecobppiid,
  `artiva_stgpspeeconbase`.pspeecobsource,
  `artiva_stgpspeeconbase`.pspeecobtime,
  `artiva_stgpspeeconbase`.pspeecobuser
  FROM
    edwpsc_base_views.`artiva_stgpspeeconbase`
;