CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refregion`
AS SELECT
  `ecw_refregion`.regionkey,
  `ecw_refregion`.regionname,
  `ecw_refregion`.regionsystemname,
  `ecw_refregion`.regiondbsnapshotname,
  `ecw_refregion`.regiondblivename,
  `ecw_refregion`.regiondbcdcname,
  `ecw_refregion`.regionactive,
  `ecw_refregion`.lastetlupdate,
  `ecw_refregion`.dwlastupdatedatetime,
  `ecw_refregion`.sourcesystemcode,
  `ecw_refregion`.insertedby,
  `ecw_refregion`.inserteddtm,
  `ecw_refregion`.modifiedby,
  `ecw_refregion`.modifieddtm,
  `ecw_refregion`.regionservername,
  `ecw_refregion`.regionssrsstagepackage,
  `ecw_refregion`.regionlastrunstagecompleteflag,
  `ecw_refregion`.regionlastrunstagefailuremessage,
  `ecw_refregion`.regionredirectlogpath,
  `ecw_refregion`.regionprefix,
  `ecw_refregion`.timezonedifference,
  `ecw_refregion`.valescoindicator,
  `ecw_refregion`.accountprefix
  FROM
    edwpsc.`ecw_refregion`
;