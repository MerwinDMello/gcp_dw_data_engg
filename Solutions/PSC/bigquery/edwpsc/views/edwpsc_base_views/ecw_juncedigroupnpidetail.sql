CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncedigroupnpidetail`
AS SELECT
  `ecw_juncedigroupnpidetail`.ecwedigroupnpidetailkey,
  `ecw_juncedigroupnpidetail`.regionkey,
  `ecw_juncedigroupnpidetail`.npiruleid,
  `ecw_juncedigroupnpidetail`.providerid,
  `ecw_juncedigroupnpidetail`.providerkey,
  `ecw_juncedigroupnpidetail`.facilityid,
  `ecw_juncedigroupnpidetail`.facilitykey,
  `ecw_juncedigroupnpidetail`.deleteflag,
  `ecw_juncedigroupnpidetail`.sourceprimarykeyvalue,
  `ecw_juncedigroupnpidetail`.dwlastupdatedatetime,
  `ecw_juncedigroupnpidetail`.sourcesystemcode,
  `ecw_juncedigroupnpidetail`.insertedby,
  `ecw_juncedigroupnpidetail`.inserteddtm,
  `ecw_juncedigroupnpidetail`.modifiedby,
  `ecw_juncedigroupnpidetail`.modifieddtm
  FROM
    edwpsc.`ecw_juncedigroupnpidetail`
;