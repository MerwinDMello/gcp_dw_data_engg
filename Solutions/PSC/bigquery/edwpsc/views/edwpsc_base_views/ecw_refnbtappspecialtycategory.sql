CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refnbtappspecialtycategory`
AS SELECT
  `ecw_refnbtappspecialtycategory`.appspecialtycategoryid,
  `ecw_refnbtappspecialtycategory`.appspecialtycategorydescription,
  `ecw_refnbtappspecialtycategory`.isactive,
  `ecw_refnbtappspecialtycategory`.insertedby,
  `ecw_refnbtappspecialtycategory`.inserteddtm,
  `ecw_refnbtappspecialtycategory`.modifiedby,
  `ecw_refnbtappspecialtycategory`.modifieddtm,
  `ecw_refnbtappspecialtycategory`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_refnbtappspecialtycategory`
;