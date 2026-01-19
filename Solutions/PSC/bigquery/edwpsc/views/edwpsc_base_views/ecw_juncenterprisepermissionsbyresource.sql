CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncenterprisepermissionsbyresource`
AS SELECT
  `ecw_juncenterprisepermissionsbyresource`.ecwenterprisepermissionsbyresourcekey,
  `ecw_juncenterprisepermissionsbyresource`.regionkey,
  `ecw_juncenterprisepermissionsbyresource`.resourcetype,
  `ecw_juncenterprisepermissionsbyresource`.resourceid,
  `ecw_juncenterprisepermissionsbyresource`.enterpriseid,
  `ecw_juncenterprisepermissionsbyresource`.sourceprimarykeyvalue,
  `ecw_juncenterprisepermissionsbyresource`.dwlastupdatedatetime,
  `ecw_juncenterprisepermissionsbyresource`.sourcesystemcode,
  `ecw_juncenterprisepermissionsbyresource`.insertedby,
  `ecw_juncenterprisepermissionsbyresource`.inserteddtm,
  `ecw_juncenterprisepermissionsbyresource`.modifiedby,
  `ecw_juncenterprisepermissionsbyresource`.modifieddtm
  FROM
    edwpsc.`ecw_juncenterprisepermissionsbyresource`
;