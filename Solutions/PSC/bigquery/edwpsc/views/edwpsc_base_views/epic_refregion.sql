CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refregion`
AS SELECT
  `epic_refregion`.regionkey,
  `epic_refregion`.regionname,
  `epic_refregion`.regionsourceconnectionstring,
  `epic_refregion`.regionsourceactive,
  `epic_refregion`.runorder,
  `epic_refregion`.regiondbname,
  `epic_refregion`.regionservername,
  `epic_refregion`.regionssrsstagepackage,
  `epic_refregion`.regionlastrunstagecompleteflag,
  `epic_refregion`.regionlastrunstagefailuremessage,
  `epic_refregion`.regionredirectlogpath,
  `epic_refregion`.timezonedifference
  FROM
    edwpsc.`epic_refregion`
;