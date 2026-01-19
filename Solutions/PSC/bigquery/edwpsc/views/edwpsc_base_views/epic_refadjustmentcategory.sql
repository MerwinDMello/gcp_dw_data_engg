CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refadjustmentcategory`
AS SELECT
  `epic_refadjustmentcategory`.adjustmentcategorykey,
  `epic_refadjustmentcategory`.adjcategoryname,
  `epic_refadjustmentcategory`.dwlastupdatedatetime,
  `epic_refadjustmentcategory`.sourcesystemcode,
  `epic_refadjustmentcategory`.insertedby,
  `epic_refadjustmentcategory`.inserteddtm,
  `epic_refadjustmentcategory`.modifiedby,
  `epic_refadjustmentcategory`.modifieddtm
  FROM
    edwpsc.`epic_refadjustmentcategory`
;