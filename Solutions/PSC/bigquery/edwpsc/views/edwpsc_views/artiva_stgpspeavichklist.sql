CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeavichklist`
AS SELECT
  `artiva_stgpspeavichklist`.pspeavichkaviid,
  `artiva_stgpspeavichklist`.pspeavichkcomp,
  `artiva_stgpspeavichklist`.pspeavichkcompdte,
  `artiva_stgpspeavichklist`.pspeavichkgafid,
  `artiva_stgpspeavichklist`.pspeavichkkey,
  `artiva_stgpspeavichklist`.pspeavichkorigqkey,
  `artiva_stgpspeavichklist`.pspeavichkperfid,
  `artiva_stgpspeavichklist`.pspeavichkresponse,
  `artiva_stgpspeavichklist`.pspeavichktask
  FROM
    edwpsc_base_views.`artiva_stgpspeavichklist`
;