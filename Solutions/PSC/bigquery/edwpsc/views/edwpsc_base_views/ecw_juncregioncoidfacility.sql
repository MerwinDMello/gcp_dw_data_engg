CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncregioncoidfacility`
AS SELECT
  `ecw_juncregioncoidfacility`.juncregioncoidfacilitykey,
  `ecw_juncregioncoidfacility`.regionkey,
  `ecw_juncregioncoidfacility`.coid,
  `ecw_juncregioncoidfacility`.facilitykey,
  `ecw_juncregioncoidfacility`.insertedby,
  `ecw_juncregioncoidfacility`.inserteddtm,
  `ecw_juncregioncoidfacility`.modifiedby,
  `ecw_juncregioncoidfacility`.modifieddtm,
  `ecw_juncregioncoidfacility`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncregioncoidfacility`
;