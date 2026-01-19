CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncprtxidsfacility`
AS SELECT
  `ecw_juncprtxidsfacility`.ecwprtxidsfacilitykey,
  `ecw_juncprtxidsfacility`.regionkey,
  `ecw_juncprtxidsfacility`.prtxruleid,
  `ecw_juncprtxidsfacility`.facilityid,
  `ecw_juncprtxidsfacility`.facilitykey,
  `ecw_juncprtxidsfacility`.deleteflag,
  `ecw_juncprtxidsfacility`.sourceprimarykeyvalue,
  `ecw_juncprtxidsfacility`.dwlastupdatedatetime,
  `ecw_juncprtxidsfacility`.sourcesystemcode,
  `ecw_juncprtxidsfacility`.insertedby,
  `ecw_juncprtxidsfacility`.inserteddtm,
  `ecw_juncprtxidsfacility`.modifiedby,
  `ecw_juncprtxidsfacility`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_juncprtxidsfacility`
;