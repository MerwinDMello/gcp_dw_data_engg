CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_reffacilityresource`
AS SELECT
  `ecw_reffacilityresource`.facilityresourcekey,
  `ecw_reffacilityresource`.facilityresourcename,
  `ecw_reffacilityresource`.facilityresourceusertype,
  `ecw_reffacilityresource`.sourceaprimarykeyvalue,
  `ecw_reffacilityresource`.sourcearecordlastupdated,
  `ecw_reffacilityresource`.sourcebrecordlastupdated,
  `ecw_reffacilityresource`.dwlastupdatedatetime,
  `ecw_reffacilityresource`.sourcesystemcode,
  `ecw_reffacilityresource`.insertedby,
  `ecw_reffacilityresource`.inserteddtm,
  `ecw_reffacilityresource`.modifiedby,
  `ecw_reffacilityresource`.modifieddtm,
  `ecw_reffacilityresource`.deleteflag
  FROM
    edwpsc.`ecw_reffacilityresource`
;