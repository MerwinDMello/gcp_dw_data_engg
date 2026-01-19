CREATE OR REPLACE VIEW edwpsc_views.`pk_refregion`
AS SELECT
  `pk_refregion`.pkregionkey,
  `pk_refregion`.pkregionname,
  `pk_refregion`.pkserverreferencename,
  `pk_refregion`.pkregionactiveflag,
  `pk_refregion`.sourcesystemcode,
  `pk_refregion`.insertedby,
  `pk_refregion`.inserteddtm,
  `pk_refregion`.modifiedby,
  `pk_refregion`.modifieddtm,
  `pk_refregion`.regionname,
  `pk_refregion`.regionsourceconnectionstring,
  `pk_refregion`.regionsourceactive,
  `pk_refregion`.regiondbname,
  `pk_refregion`.regionservername,
  `pk_refregion`.regionssrsstagepackage,
  `pk_refregion`.regionlastrunstagecompleteflag,
  `pk_refregion`.regionlastrunstagefailuremessage,
  `pk_refregion`.regionredirectlogpath,
  `pk_refregion`.schemaname,
  `pk_refregion`.environmentid,
  `pk_refregion`.miscrunflag,
  `pk_refregion`.timezonedifference,
  `pk_refregion`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pk_refregion`
;