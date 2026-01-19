CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeppi_pspeppinotes`
AS SELECT
  `artiva_stgpspeppi_pspeppinotes`.note_cnt,
  `artiva_stgpspeppi_pspeppinotes`.note_date,
  `artiva_stgpspeppi_pspeppinotes`.note_time,
  `artiva_stgpspeppi_pspeppinotes`.note_type,
  `artiva_stgpspeppi_pspeppinotes`.note_user,
  `artiva_stgpspeppi_pspeppinotes`.pspeppikey,
  `artiva_stgpspeppi_pspeppinotes`.pspeppinotes,
  `artiva_stgpspeppi_pspeppinotes`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`artiva_stgpspeppi_pspeppinotes`
;