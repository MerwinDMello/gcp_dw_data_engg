CREATE OR REPLACE VIEW edwpsc_base_views.`epic_juncfacilityresourcecoid`
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
    edwpsc.`epic_juncfacilityresourcecoid`
;