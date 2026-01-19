CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspespausers`
AS SELECT
  `artiva_stgpspespausers`.pspesparelukey,
  `artiva_stgpspespausers`.pspespareluser,
  `artiva_stgpspespausers`.pspespareluspaid
  FROM
    edwpsc_base_views.`artiva_stgpspespausers`
;