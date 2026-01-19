CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refnbtspecialtycategory`
AS SELECT
  `ecw_refnbtspecialtycategory`.nbtspecialtycategoryidkey,
  `ecw_refnbtspecialtycategory`.nbtspecialtycategoryid,
  `ecw_refnbtspecialtycategory`.nbtspecialtycategorydesc,
  `ecw_refnbtspecialtycategory`.active,
  `ecw_refnbtspecialtycategory`.dwlastupdatedatetime,
  `ecw_refnbtspecialtycategory`.sourcesystemcode,
  `ecw_refnbtspecialtycategory`.insertedby,
  `ecw_refnbtspecialtycategory`.inserteddtm,
  `ecw_refnbtspecialtycategory`.modifiedby,
  `ecw_refnbtspecialtycategory`.modifieddtm
  FROM
    edwpsc.`ecw_refnbtspecialtycategory`
;