CREATE OR REPLACE VIEW edwpsc_base_views.`pv_juncprovidercoid`
AS SELECT
  `pv_juncprovidercoid`.juncprovidercoid,
  `pv_juncprovidercoid`.providerkey,
  `pv_juncprovidercoid`.coid,
  `pv_juncprovidercoid`.insertedby,
  `pv_juncprovidercoid`.inserteddtm,
  `pv_juncprovidercoid`.modifiedby,
  `pv_juncprovidercoid`.modifieddtm,
  `pv_juncprovidercoid`.providertype,
  `pv_juncprovidercoid`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_juncprovidercoid`
;