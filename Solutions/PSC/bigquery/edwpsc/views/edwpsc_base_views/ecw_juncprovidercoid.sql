CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncprovidercoid`
AS SELECT
  `ecw_juncprovidercoid`.juncprovidercoid,
  `ecw_juncprovidercoid`.providerkey,
  `ecw_juncprovidercoid`.coid,
  `ecw_juncprovidercoid`.insertedby,
  `ecw_juncprovidercoid`.inserteddtm,
  `ecw_juncprovidercoid`.modifiedby,
  `ecw_juncprovidercoid`.modifieddtm,
  `ecw_juncprovidercoid`.providertype,
  `ecw_juncprovidercoid`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncprovidercoid`
;