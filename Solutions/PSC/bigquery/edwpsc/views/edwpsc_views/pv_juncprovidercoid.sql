CREATE OR REPLACE VIEW edwpsc_views.`pv_juncprovidercoid`
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
    edwpsc_base_views.`pv_juncprovidercoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_juncprovidercoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;