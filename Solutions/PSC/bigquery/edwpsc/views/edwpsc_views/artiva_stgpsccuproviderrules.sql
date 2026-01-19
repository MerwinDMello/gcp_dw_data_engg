CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpsccuproviderrules`
AS SELECT
  `artiva_stgpsccuproviderrules`.psccuprrulekey,
  `artiva_stgpsccuproviderrules`.psccuprrulconfeffdte,
  `artiva_stgpsccuproviderrules`.psccuprrulconfigkey,
  `artiva_stgpsccuproviderrules`.psccuprrulconfiglbl,
  `artiva_stgpsccuproviderrules`.psccuprrulconflkflg,
  `artiva_stgpsccuproviderrules`.psccuprrulcontrmdte,
  `artiva_stgpsccuproviderrules`.psccuprruleid,
  `artiva_stgpsccuproviderrules`.psccuprrulinvowner,
  `artiva_stgpsccuproviderrules`.psccuprrulrequestor,
  `artiva_stgpsccuproviderrules`.sourcesystemcode,
  `artiva_stgpsccuproviderrules`.dwlastupdatedatetime,
  `artiva_stgpsccuproviderrules`.insertedby,
  `artiva_stgpsccuproviderrules`.inserteddtm,
  `artiva_stgpsccuproviderrules`.modifiedby,
  `artiva_stgpsccuproviderrules`.modifieddtm
  FROM
    edwpsc_base_views.`artiva_stgpsccuproviderrules`
;