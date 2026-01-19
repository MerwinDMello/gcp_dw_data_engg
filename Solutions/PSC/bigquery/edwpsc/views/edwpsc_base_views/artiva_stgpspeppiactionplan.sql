CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspeppiactionplan`
AS SELECT
  `artiva_stgpspeppiactionplan`.pspeppiapincflag,
  `artiva_stgpspeppiactionplan`.pspeppiapkey,
  `artiva_stgpspeppiactionplan`.pspeppiaporigdte,
  `artiva_stgpspeppiactionplan`.pspeppiappanels,
  `artiva_stgpspeppiactionplan`.pspeppiappayid,
  `artiva_stgpspeppiactionplan`.pspeppiapppicrtdte,
  `artiva_stgpspeppiactionplan`.pspeppiapppiid,
  `artiva_stgpspeppiactionplan`.pspeppiapspatmpid,
  `artiva_stgpspeppiactionplan`.pspeppiapsubdte,
  `artiva_stgpspeppiactionplan`.pspeppiaprejreason
  FROM
    edwpsc.`artiva_stgpspeppiactionplan`
;