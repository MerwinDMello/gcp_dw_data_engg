CREATE OR REPLACE VIEW edwpsc_views.`ecw_reffacilitymeditechexpanse`
AS SELECT
  `ecw_reffacilitymeditechexpanse`.facilitykey,
  `ecw_reffacilitymeditechexpanse`.regionkey,
  `ecw_reffacilitymeditechexpanse`.facilityname,
  `ecw_reffacilitymeditechexpanse`.facilityaccountnumberprefix,
  `ecw_reffacilitymeditechexpanse`.deleteflag,
  `ecw_reffacilitymeditechexpanse`.sourceprimarykeyvalue,
  `ecw_reffacilitymeditechexpanse`.sourcearecordlastupdated,
  `ecw_reffacilitymeditechexpanse`.dwlastupdatedatetime,
  `ecw_reffacilitymeditechexpanse`.sourcesystemcode,
  `ecw_reffacilitymeditechexpanse`.insertedby,
  `ecw_reffacilitymeditechexpanse`.inserteddtm,
  `ecw_reffacilitymeditechexpanse`.modifiedby,
  `ecw_reffacilitymeditechexpanse`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_reffacilitymeditechexpanse`
;