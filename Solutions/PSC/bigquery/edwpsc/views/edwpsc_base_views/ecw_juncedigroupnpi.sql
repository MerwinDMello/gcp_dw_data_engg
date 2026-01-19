CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncedigroupnpi`
AS SELECT
  `ecw_juncedigroupnpi`.ecwedigroupnpikey,
  `ecw_juncedigroupnpi`.regionkey,
  `ecw_juncedigroupnpi`.npirulename,
  `ecw_juncedigroupnpi`.groupnpi,
  `ecw_juncedigroupnpi`.deleteflag,
  `ecw_juncedigroupnpi`.allapptfacilities,
  `ecw_juncedigroupnpi`.allpracticingproviders,
  `ecw_juncedigroupnpi`.selectiveservicedates,
  `ecw_juncedigroupnpi`.startservicedate,
  `ecw_juncedigroupnpi`.endservicedate,
  `ecw_juncedigroupnpi`.allfacilities,
  `ecw_juncedigroupnpi`.allpracticingfacilities,
  `ecw_juncedigroupnpi`.allnonpracticingfacilities,
  `ecw_juncedigroupnpi`.selectiveinsurances,
  `ecw_juncedigroupnpi`.sourceprimarykeyvalue,
  `ecw_juncedigroupnpi`.dwlastupdatedatetime,
  `ecw_juncedigroupnpi`.sourcesystemcode,
  `ecw_juncedigroupnpi`.insertedby,
  `ecw_juncedigroupnpi`.inserteddtm,
  `ecw_juncedigroupnpi`.modifiedby,
  `ecw_juncedigroupnpi`.modifieddtm
  FROM
    edwpsc.`ecw_juncedigroupnpi`
;