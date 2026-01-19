CREATE OR REPLACE VIEW edwpsc_base_views.`epic_juncmarketcoid`
AS SELECT
  `epic_juncmarketcoid`.juncmarketcoidkey,
  `epic_juncmarketcoid`.marketkey,
  `epic_juncmarketcoid`.coid,
  `epic_juncmarketcoid`.insertedby,
  `epic_juncmarketcoid`.inserteddtm,
  `epic_juncmarketcoid`.modifiedby,
  `epic_juncmarketcoid`.modifieddtm,
  `epic_juncmarketcoid`.dwlastupdatedatetime
  FROM
    edwpsc.`epic_juncmarketcoid`
;