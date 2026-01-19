CREATE OR REPLACE VIEW edwpsc_views.`pv_juncfacilityresourcecoid`
AS SELECT
  `pv_juncfacilityresourcecoid`.juncfacilityresourcecoidkey,
  `pv_juncfacilityresourcecoid`.facilityresourcekey,
  `pv_juncfacilityresourcecoid`.coid,
  `pv_juncfacilityresourcecoid`.insertedby,
  `pv_juncfacilityresourcecoid`.inserteddtm,
  `pv_juncfacilityresourcecoid`.modifiedby,
  `pv_juncfacilityresourcecoid`.modifieddtm,
  `pv_juncfacilityresourcecoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_juncfacilityresourcecoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_juncfacilityresourcecoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;