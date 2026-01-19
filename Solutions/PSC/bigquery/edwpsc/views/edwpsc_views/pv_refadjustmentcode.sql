CREATE OR REPLACE VIEW edwpsc_views.`pv_refadjustmentcode`
AS SELECT
  `pv_refadjustmentcode`.adjustmentcodekey,
  `pv_refadjustmentcode`.adjcode,
  `pv_refadjustmentcode`.adjname,
  `pv_refadjustmentcode`.adjdescription,
  `pv_refadjustmentcode`.adjustmentcategorykey,
  `pv_refadjustmentcode`.dwlastupdatedatetime,
  `pv_refadjustmentcode`.sourcesystemcode,
  `pv_refadjustmentcode`.insertedby,
  `pv_refadjustmentcode`.inserteddtm,
  `pv_refadjustmentcode`.modifiedby,
  `pv_refadjustmentcode`.modifieddtm,
  `pv_refadjustmentcode`.nonparflag,
  `pv_refadjustmentcode`.billableflag,
  `pv_refadjustmentcode`.adjsubcategoryname
  FROM
    edwpsc_base_views.`pv_refadjustmentcode`
;