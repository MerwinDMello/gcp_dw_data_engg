CREATE OR REPLACE VIEW edwpsc_base_views.`epic_juncprovidercoid`
AS SELECT
  `epic_juncprovidercoid`.juncprovidercoid,
  `epic_juncprovidercoid`.providerkey,
  `epic_juncprovidercoid`.coid,
  `epic_juncprovidercoid`.insertedby,
  `epic_juncprovidercoid`.inserteddtm,
  `epic_juncprovidercoid`.modifiedby,
  `epic_juncprovidercoid`.modifieddtm,
  `epic_juncprovidercoid`.providertype,
  `epic_juncprovidercoid`.dwlastupdatedatetime
  FROM
    edwpsc.`epic_juncprovidercoid`
;