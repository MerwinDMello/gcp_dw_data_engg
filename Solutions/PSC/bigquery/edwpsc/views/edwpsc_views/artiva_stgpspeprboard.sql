CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeprboard`
AS SELECT
  `artiva_stgpspeprboard`.pspeprbdcertboard,
  `artiva_stgpspeprboard`.pspeprbdcertdte,
  `artiva_stgpspeprboard`.pspeprbdcertmaint,
  `artiva_stgpspeprboard`.pspeprbdcertnum,
  `artiva_stgpspeprboard`.pspeprbdexpdte,
  `artiva_stgpspeprboard`.pspeprbdgafid,
  `artiva_stgpspeprboard`.pspeprbdkey,
  `artiva_stgpspeprboard`.pspeprbdlcert,
  `artiva_stgpspeprboard`.pspeprbdperfid,
  `artiva_stgpspeprboard`.pspeprbdrecertdte,
  `artiva_stgpspeprboard`.pspeprbdrevdte,
  `artiva_stgpspeprboard`.pspeprbdstatus
  FROM
    edwpsc_base_views.`artiva_stgpspeprboard`
;