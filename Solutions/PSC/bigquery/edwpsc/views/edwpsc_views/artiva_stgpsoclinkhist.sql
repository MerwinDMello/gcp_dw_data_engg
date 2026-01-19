CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpsoclinkhist`
AS SELECT
  `artiva_stgpsoclinkhist`.psoclhkey,
  `artiva_stgpsoclinkhist`.psoclhaction,
  `artiva_stgpsoclinkhist`.psoclhdate,
  `artiva_stgpsoclinkhist`.psoclhdesc,
  `artiva_stgpsoclinkhist`.psoclhlead,
  `artiva_stgpsoclinkhist`.psoclhlinkid,
  `artiva_stgpsoclinkhist`.psoclhmsgid,
  `artiva_stgpsoclinkhist`.psoclhtime,
  `artiva_stgpsoclinkhist`.psoclhuser,
  `artiva_stgpsoclinkhist`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`artiva_stgpsoclinkhist`
;