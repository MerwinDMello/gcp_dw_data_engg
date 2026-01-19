CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeppilinks`
AS SELECT
  `artiva_stgpspeppilinks`.pspeliaviid,
  `artiva_stgpspeppilinks`.pspelicount,
  `artiva_stgpspeppilinks`.pspelidate,
  `artiva_stgpspeppilinks`.pspelidesc,
  `artiva_stgpspeppilinks`.pspelikey,
  `artiva_stgpspeppilinks`.pspelileadppi,
  `artiva_stgpspeppilinks`.pspeliprod,
  `artiva_stgpspeppilinks`.pspeliuser,
  `artiva_stgpspeppilinks`.pspeliuserchgdte
  FROM
    edwpsc_base_views.`artiva_stgpspeppilinks`
;