CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeppihist`
AS SELECT
  `artiva_stgpspeppihist`.pspeppihaction,
  `artiva_stgpspeppihist`.pspeppihaviid,
  `artiva_stgpspeppihist`.pspeppihdate,
  `artiva_stgpspeppihist`.pspeppihfromamtval,
  `artiva_stgpspeppihist`.pspeppihfromcalc,
  `artiva_stgpspeppihist`.pspeppihfromdateval,
  `artiva_stgpspeppihist`.pspeppihfromval,
  `artiva_stgpspeppihist`.pspeppihkey,
  `artiva_stgpspeppihist`.pspeppihlpool,
  `artiva_stgpspeppihist`.pspeppihperfid,
  `artiva_stgpspeppihist`.pspeppihppiid,
  `artiva_stgpspeppihist`.pspeppihtime,
  `artiva_stgpspeppihist`.pspeppihtoamtval,
  `artiva_stgpspeppihist`.pspeppihtocalc,
  `artiva_stgpspeppihist`.pspeppihtodateval,
  `artiva_stgpspeppihist`.pspeppihtoval,
  `artiva_stgpspeppihist`.pspeppihuserid,
  `artiva_stgpspeppihist`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`artiva_stgpspeppihist`
;