CREATE OR REPLACE VIEW edwpsc_views.`epic_juncprovidercoid`
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
    edwpsc_base_views.`epic_juncprovidercoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_juncprovidercoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;