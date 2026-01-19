CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspespataxonpiovr`
AS SELECT
  `artiva_stgpspespataxonpiovr`.pspestnkey,
  `artiva_stgpspespataxonpiovr`.pspestnnpi,
  `artiva_stgpspespataxonpiovr`.pspestnspaid,
  `artiva_stgpspespataxonpiovr`.pspestntaxonomy
  FROM
    edwpsc_base_views.`artiva_stgpspespataxonpiovr`
;