CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspetaxonomycodes`
AS SELECT
  `artiva_stgpspetaxonomycodes`.pspetaxdesc,
  `artiva_stgpspetaxonomycodes`.pspetaxkey,
  `artiva_stgpspetaxonomycodes`.pspetaxmcrspccode,
  `artiva_stgpspetaxonomycodes`.pspetaxmcrsuptypdesc
  FROM
    edwpsc.`artiva_stgpspetaxonomycodes`
;