CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpsadjcodes`
AS SELECT
  `artiva_stgpsadjcodes`.psadactive,
  `artiva_stgpsadjcodes`.psadapprvlreq,
  `artiva_stgpsadjcodes`.psaddesc,
  `artiva_stgpsadjcodes`.psadid,
  `artiva_stgpsadjcodes`.psadsec,
  `artiva_stgpsadjcodes`.psadtype
  FROM
    edwpsc_base_views.`artiva_stgpsadjcodes`
;