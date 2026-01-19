CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeprlicense`
AS SELECT
  `artiva_stgpspeprlicense`.pspeprliawarddte,
  `artiva_stgpspeprlicense`.pspeprlienrollstat,
  `artiva_stgpspeprlicense`.pspeprlienstasdte,
  `artiva_stgpspeprlicense`.pspeprliexpdte,
  `artiva_stgpspeprlicense`.pspeprligafid,
  `artiva_stgpspeprlicense`.pspeprliinid,
  `artiva_stgpspeprlicense`.pspeprlikey,
  `artiva_stgpspeprlicense`.pspeprlinumber,
  `artiva_stgpspeprlicense`.pspeprliperfid,
  `artiva_stgpspeprlicense`.pspeprlipubstasdte,
  `artiva_stgpspeprlicense`.pspeprlipubstat,
  `artiva_stgpspeprlicense`.pspeprlipubstliteral,
  `artiva_stgpspeprlicense`.pspeprlistate,
  `artiva_stgpspeprlicense`.pspeprlistatus,
  `artiva_stgpspeprlicense`.pspeprlitype
  FROM
    edwpsc_base_views.`artiva_stgpspeprlicense`
;