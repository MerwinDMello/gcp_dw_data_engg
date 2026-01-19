CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpsoctransactions`
AS SELECT
  `artiva_stgpsoctransactions`.psoctrkey,
  `artiva_stgpsoctransactions`.psoctrcat,
  `artiva_stgpsoctransactions`.psoctrdate,
  `artiva_stgpsoctransactions`.psoctrloaddte,
  `artiva_stgpsoctransactions`.psoctrmod1,
  `artiva_stgpsoctransactions`.psoctrmod2,
  `artiva_stgpsoctransactions`.psoctrmod3,
  `artiva_stgpsoctransactions`.psoctrmod4,
  `artiva_stgpsoctransactions`.psoctrocid,
  `artiva_stgpsoctransactions`.psoctrorigdte,
  `artiva_stgpsoctransactions`.psoctrproccd,
  `artiva_stgpsoctransactions`.psoctrproccdid,
  `artiva_stgpsoctransactions`.dwlastupdatedatetime
  FROM
    edwpsc.`artiva_stgpsoctransactions`
;