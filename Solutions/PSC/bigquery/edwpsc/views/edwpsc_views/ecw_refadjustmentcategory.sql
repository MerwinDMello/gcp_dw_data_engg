CREATE OR REPLACE VIEW edwpsc_views.`ecw_refadjustmentcategory`
AS SELECT
  `ecw_refadjustmentcategory`.adjustmentcategorykey,
  `ecw_refadjustmentcategory`.adjcategoryname,
  `ecw_refadjustmentcategory`.dwlastupdatedatetime,
  `ecw_refadjustmentcategory`.sourcesystemcode,
  `ecw_refadjustmentcategory`.insertedby,
  `ecw_refadjustmentcategory`.inserteddtm,
  `ecw_refadjustmentcategory`.modifiedby,
  `ecw_refadjustmentcategory`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refadjustmentcategory`
;