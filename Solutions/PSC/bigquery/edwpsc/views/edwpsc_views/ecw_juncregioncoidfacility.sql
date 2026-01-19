CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncregioncoidfacility`
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
    edwpsc_base_views.`ecw_juncregioncoidfacility`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncregioncoidfacility`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;