CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspespautility`
AS SELECT
  `artiva_stgpspespautility`.pspespacounty,
  `artiva_stgpspespautility`.pspespacrtdte,
  `artiva_stgpspespautility`.pspespacrttime,
  `artiva_stgpspespautility`.pspespakey,
  `artiva_stgpspespautility`.pspespalbn,
  `artiva_stgpspespautility`.pspespaprocessflg,
  `artiva_stgpspespautility`.pspespaspecialty,
  `artiva_stgpspespautility`.pspespasspdivision,
  `artiva_stgpspespautility`.pspespastate,
  `artiva_stgpspespautility`.pspespataxid,
  `artiva_stgpspespautility`.pspespataxonomy
  FROM
    edwpsc.`artiva_stgpspespautility`
;