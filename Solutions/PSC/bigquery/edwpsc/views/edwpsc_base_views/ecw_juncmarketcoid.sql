CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncmarketcoid`
AS SELECT
  `ecw_juncmarketcoid`.juncmarketcoidkey,
  `ecw_juncmarketcoid`.marketkey,
  `ecw_juncmarketcoid`.coid,
  `ecw_juncmarketcoid`.insertedby,
  `ecw_juncmarketcoid`.inserteddtm,
  `ecw_juncmarketcoid`.modifiedby,
  `ecw_juncmarketcoid`.modifieddtm,
  `ecw_juncmarketcoid`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncmarketcoid`
;