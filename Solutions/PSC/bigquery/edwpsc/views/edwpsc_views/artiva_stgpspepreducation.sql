CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspepreducation`
AS SELECT
  `artiva_stgpspepreducation`.pspepredcertnum,
  `artiva_stgpspepreducation`.pspepreddegree,
  `artiva_stgpspepreducation`.pspepredenddte,
  `artiva_stgpspepreducation`.pspepredgafid,
  `artiva_stgpspepreducation`.pspepredinid,
  `artiva_stgpspepreducation`.pspepredkey,
  `artiva_stgpspepreducation`.pspepredperfid,
  `artiva_stgpspepreducation`.pspepredprogram,
  `artiva_stgpspepreducation`.pspepredspecialty,
  `artiva_stgpspepreducation`.pspepredstdte,
  `artiva_stgpspepreducation`.pspepredtitle
  FROM
    edwpsc_base_views.`artiva_stgpspepreducation`
;