CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncfacilityresourcecoid`
AS SELECT
  `ecw_juncfacilityresourcecoid`.juncfacilityresourcecoidkey,
  `ecw_juncfacilityresourcecoid`.facilityresourcekey,
  `ecw_juncfacilityresourcecoid`.coid,
  `ecw_juncfacilityresourcecoid`.insertedby,
  `ecw_juncfacilityresourcecoid`.inserteddtm,
  `ecw_juncfacilityresourcecoid`.modifiedby,
  `ecw_juncfacilityresourcecoid`.modifieddtm,
  `ecw_juncfacilityresourcecoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_juncfacilityresourcecoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncfacilityresourcecoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;