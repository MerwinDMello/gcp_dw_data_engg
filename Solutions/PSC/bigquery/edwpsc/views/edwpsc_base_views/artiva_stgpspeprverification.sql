CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspeprverification`
AS SELECT
  `artiva_stgpspeprverification`.pspeprvafid,
  `artiva_stgpspeprverification`.pspeprvaviid,
  `artiva_stgpspeprverification`.pspeprvbarcode,
  `artiva_stgpspeprverification`.pspeprvbdid,
  `artiva_stgpspeprverification`.pspeprvedid,
  `artiva_stgpspeprverification`.pspeprvgafid,
  `artiva_stgpspeprverification`.pspeprvhowv,
  `artiva_stgpspeprverification`.pspeprvinsid,
  `artiva_stgpspeprverification`.pspeprvlicid,
  `artiva_stgpspeprverification`.pspeprvperfid,
  `artiva_stgpspeprverification`.pspeprvverified,
  `artiva_stgpspeprverification`.pspeprvverifydte,
  `artiva_stgpspeprverification`.pspeprvverifyuser,
  `artiva_stgpspeprverification`.pspeprvkey
  FROM
    edwpsc.`artiva_stgpspeprverification`
;