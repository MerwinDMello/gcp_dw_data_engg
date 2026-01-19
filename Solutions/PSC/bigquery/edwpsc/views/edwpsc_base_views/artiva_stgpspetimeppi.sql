CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspetimeppi`
AS SELECT
  `artiva_stgpspetimeppi`.pspetimeaviid,
  `artiva_stgpspetimeppi`.pspetimedate,
  `artiva_stgpspetimeppi`.pspetimeetime,
  `artiva_stgpspetimeppi`.pspetimekey,
  `artiva_stgpspetimeppi`.pspetimeperfid,
  `artiva_stgpspetimeppi`.pspetimeppiid,
  `artiva_stgpspetimeppi`.pspetimestime,
  `artiva_stgpspetimeppi`.pspetimetime,
  `artiva_stgpspetimeppi`.pspetimettime,
  `artiva_stgpspetimeppi`.pspetimeuser,
  `artiva_stgpspetimeppi`.pspetimeworked,
  `artiva_stgpspetimeppi`.dwlastupdatedatetime
  FROM
    edwpsc.`artiva_stgpspetimeppi`
;