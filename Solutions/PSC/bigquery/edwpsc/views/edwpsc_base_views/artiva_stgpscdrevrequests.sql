CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpscdrevrequests`
AS SELECT
  `artiva_stgpscdrevrequests`.pscdracctid,
  `artiva_stgpscdrevrequests`.pscdrcrtdte,
  `artiva_stgpscdrevrequests`.pscdrcrttime,
  `artiva_stgpscdrevrequests`.pscdrcrtuser,
  `artiva_stgpscdrevrequests`.pscdrencntrid,
  `artiva_stgpscdrevrequests`.pscdrkey,
  `artiva_stgpscdrevrequests`.pscdrstatus,
  `artiva_stgpscdrevrequests`.pscdrutilityid
  FROM
    edwpsc.`artiva_stgpscdrevrequests`
;