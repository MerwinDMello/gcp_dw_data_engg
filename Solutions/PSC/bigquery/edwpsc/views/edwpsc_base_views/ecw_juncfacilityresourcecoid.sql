CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncfacilityresourcecoid`
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
    edwpsc.`ecw_juncfacilityresourcecoid`
;