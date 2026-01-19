CREATE OR REPLACE VIEW edwpsc_views.`pv_refregion`
AS SELECT
  `pv_refregion`.regionkey,
  `pv_refregion`.regionname,
  `pv_refregion`.regionsourceconnectionstring,
  `pv_refregion`.regionsourceactive,
  `pv_refregion`.artivaregionkey,
  `pv_refregion`.regiondbname,
  `pv_refregion`.regionservername,
  `pv_refregion`.regionssrsstagepackage,
  `pv_refregion`.regionlastrunstagecompleteflag,
  `pv_refregion`.regionlastrunstagefailuremessage,
  `pv_refregion`.regionredirectlogpath
  FROM
    edwpsc_base_views.`pv_refregion`
;