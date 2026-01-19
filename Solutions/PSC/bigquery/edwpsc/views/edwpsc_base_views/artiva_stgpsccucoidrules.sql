CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpsccucoidrules`
AS SELECT
  `artiva_stgpsccucoidrules`.psccucoidrulekey,
  `artiva_stgpsccucoidrules`.psccucoidrulconeffdt,
  `artiva_stgpsccucoidrules`.psccucoidrulconfgkey,
  `artiva_stgpsccucoidrules`.psccucoidrulconfglbl,
  `artiva_stgpsccucoidrules`.psccucoidrulconlkflg,
  `artiva_stgpsccucoidrules`.psccucoidrulcontrmdt,
  `artiva_stgpsccucoidrules`.psccucoidruleid,
  `artiva_stgpsccucoidrules`.psccucoidrulinvowner,
  `artiva_stgpsccucoidrules`.psccucoidrulrequestr,
  `artiva_stgpsccucoidrules`.sourcesystemcode,
  `artiva_stgpsccucoidrules`.dwlastupdatedatetime,
  `artiva_stgpsccucoidrules`.insertedby,
  `artiva_stgpsccucoidrules`.inserteddtm,
  `artiva_stgpsccucoidrules`.modifiedby,
  `artiva_stgpsccucoidrules`.modifieddtm
  FROM
    edwpsc.`artiva_stgpsccucoidrules`
;