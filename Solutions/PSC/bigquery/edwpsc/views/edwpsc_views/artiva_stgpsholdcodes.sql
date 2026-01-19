CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpsholdcodes`
AS SELECT
  `artiva_stgpsholdcodes`.pshcdesc,
  `artiva_stgpsholdcodes`.pshcid,
  `artiva_stgpsholdcodes`.pshcmtype,
  `artiva_stgpsholdcodes`.pshcpriority,
  `artiva_stgpsholdcodes`.pshctype,
  `artiva_stgpsholdcodes`.pshcriskreport
  FROM
    edwpsc_base_views.`artiva_stgpsholdcodes`
;