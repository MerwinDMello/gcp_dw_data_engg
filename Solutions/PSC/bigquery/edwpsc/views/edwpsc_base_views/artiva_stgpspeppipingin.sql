CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspeppipingin`
AS SELECT
  `artiva_stgpspeppipingin`.pspeppipgkey,
  `artiva_stgpspeppipingin`.pspeppipgactive,
  `artiva_stgpspeppipingin`.pspeppipgeffdte,
  `artiva_stgpspeppipingin`.pspeppipggin,
  `artiva_stgpspeppipingin`.pspeppipgperfid,
  `artiva_stgpspeppipingin`.pspeppipgpin,
  `artiva_stgpspeppipingin`.pspeppipgppiid,
  `artiva_stgpspeppipingin`.pspeppipgtermdte
  FROM
    edwpsc.`artiva_stgpspeppipingin`
;