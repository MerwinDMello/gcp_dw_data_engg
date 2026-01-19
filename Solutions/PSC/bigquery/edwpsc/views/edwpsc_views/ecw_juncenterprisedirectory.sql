CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncenterprisedirectory`
AS SELECT
  `ecw_juncenterprisedirectory`.ecwenterprisedirectorykey,
  `ecw_juncenterprisedirectory`.regionkey,
  `ecw_juncenterprisedirectory`.orgid,
  `ecw_juncenterprisedirectory`.parentid,
  `ecw_juncenterprisedirectory`.orgname,
  `ecw_juncenterprisedirectory`.orgdesc,
  `ecw_juncenterprisedirectory`.orgtype,
  `ecw_juncenterprisedirectory`.deleteflag,
  `ecw_juncenterprisedirectory`.sourceprimarykeyvalue,
  `ecw_juncenterprisedirectory`.dwlastupdatedatetime,
  `ecw_juncenterprisedirectory`.sourcesystemcode,
  `ecw_juncenterprisedirectory`.insertedby,
  `ecw_juncenterprisedirectory`.inserteddtm,
  `ecw_juncenterprisedirectory`.modifiedby,
  `ecw_juncenterprisedirectory`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_juncenterprisedirectory`
;