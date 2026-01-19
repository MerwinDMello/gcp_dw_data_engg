CREATE OR REPLACE VIEW edwpsc_base_views.`pv_juncfacilityresourcecoid`
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
    edwpsc.`pv_juncfacilityresourcecoid`
;