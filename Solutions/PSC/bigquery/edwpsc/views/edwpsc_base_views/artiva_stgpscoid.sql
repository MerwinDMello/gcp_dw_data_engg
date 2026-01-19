CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpscoid`
AS SELECT
  `artiva_stgpscoid`.pscoidkey,
  `artiva_stgpscoid`.pscoidsiteid,
  `artiva_stgpscoid`.pscoidcompany,
  `artiva_stgpscoid`.pscoiddivision,
  `artiva_stgpscoid`.pscoidgroup,
  `artiva_stgpscoid`.pscoidmarket,
  `artiva_stgpscoid`.pscoidnumber,
  `artiva_stgpscoid`.pscoidregion,
  `artiva_stgpscoid`.pscoidlocationcode,
  `artiva_stgpscoid`.pscoiddeptcode,
  `artiva_stgpscoid`.pscoidpractice,
  `artiva_stgpscoid`.pscoidprovider34,
  `artiva_stgpscoid`.pscoidproviderid,
  `artiva_stgpscoid`.pscoidprovtrmdte,
  `artiva_stgpscoid`.pscoidprovtrmtype,
  `artiva_stgpscoid`.pscoidpracclsdte,
  `artiva_stgpscoid`.pscoidpracclstype,
  `artiva_stgpscoid`.pscoidietreroute,
  `artiva_stgpscoid`.pscoidietsrcedept
  FROM
    edwpsc.`artiva_stgpscoid`
;