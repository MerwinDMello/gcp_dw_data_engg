CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpscdreviewutility`
AS SELECT
  `artiva_stgpscdreviewutility`.pscdclmsel,
  `artiva_stgpscdreviewutility`.pscdcoid,
  `artiva_stgpscdreviewutility`.pscdcrtdte,
  `artiva_stgpscdreviewutility`.pscdcrttime,
  `artiva_stgpscdreviewutility`.pscdcrtuser,
  `artiva_stgpscdreviewutility`.pscddesc,
  `artiva_stgpscdreviewutility`.pscdkey,
  `artiva_stgpscdreviewutility`.pscdpercent,
  `artiva_stgpscdreviewutility`.pscdproviderid,
  `artiva_stgpscdreviewutility`.pscdstatus
  FROM
    edwpsc_base_views.`artiva_stgpscdreviewutility`
;