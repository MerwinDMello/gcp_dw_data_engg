CREATE OR REPLACE VIEW edwpsc_base_views.`pv_refadjustmentcategory`
AS SELECT
  `pv_refadjustmentcategory`.adjustmentcategorykey,
  `pv_refadjustmentcategory`.adjcategoryname,
  `pv_refadjustmentcategory`.dwlastupdatedatetime,
  `pv_refadjustmentcategory`.sourcesystemcode,
  `pv_refadjustmentcategory`.insertedby,
  `pv_refadjustmentcategory`.inserteddtm,
  `pv_refadjustmentcategory`.modifiedby,
  `pv_refadjustmentcategory`.modifieddtm
  FROM
    edwpsc.`pv_refadjustmentcategory`
;