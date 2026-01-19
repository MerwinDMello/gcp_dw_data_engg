CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspegrplocecwfac`
AS SELECT
  `artiva_stgpspegrplocecwfac`.pspegrplfecwfacid,
  `artiva_stgpspegrplocecwfac`.pspegrplfglid,
  `artiva_stgpspegrplocecwfac`.pspegrplfkey
  FROM
    edwpsc.`artiva_stgpspegrplocecwfac`
;