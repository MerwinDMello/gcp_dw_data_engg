CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeppiholdrules`
AS SELECT
  `artiva_stgpspeppiholdrules`.pspeppihrkey,
  `artiva_stgpspeppiholdrules`.pspeppihrppiid,
  `artiva_stgpspeppiholdrules`.pspeppihruid
  FROM
    edwpsc_base_views.`artiva_stgpspeppiholdrules`
;