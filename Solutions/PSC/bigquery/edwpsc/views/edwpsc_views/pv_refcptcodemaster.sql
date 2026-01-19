CREATE OR REPLACE VIEW edwpsc_views.`pv_refcptcodemaster`
AS SELECT
  `pv_refcptcodemaster`.code,
  `pv_refcptcodemaster`.codedesc,
  `pv_refcptcodemaster`.customcodeflag
  FROM
    edwpsc_base_views.`pv_refcptcodemaster`
;