CREATE OR REPLACE VIEW edwpsc_views.`epic_juncfacilityresourcecoid`
AS SELECT
  `epic_juncfacilityresourcecoid`.juncfacilityresourcecoidkey,
  `epic_juncfacilityresourcecoid`.facilityresourcekey,
  `epic_juncfacilityresourcecoid`.coid,
  `epic_juncfacilityresourcecoid`.insertedby,
  `epic_juncfacilityresourcecoid`.inserteddtm,
  `epic_juncfacilityresourcecoid`.modifiedby,
  `epic_juncfacilityresourcecoid`.modifieddtm,
  `epic_juncfacilityresourcecoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_juncfacilityresourcecoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_juncfacilityresourcecoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;