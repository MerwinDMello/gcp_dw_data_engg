CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpsoctimeoc`
AS SELECT
  `artiva_stgpsoctimeoc`.psoctimedate,
  `artiva_stgpsoctimeoc`.psoctimeetime,
  `artiva_stgpsoctimeoc`.psoctimeocid,
  `artiva_stgpsoctimeoc`.psoctimestime,
  `artiva_stgpsoctimeoc`.psoctimetime,
  `artiva_stgpsoctimeoc`.psoctimettime,
  `artiva_stgpsoctimeoc`.psoctimeuser,
  `artiva_stgpsoctimeoc`.psoctimeworked,
  `artiva_stgpsoctimeoc`.psoctimekey,
  `artiva_stgpsoctimeoc`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`artiva_stgpsoctimeoc`
;