CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncprovidercoid`
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
    edwpsc_base_views.`ecw_juncprovidercoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncprovidercoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;