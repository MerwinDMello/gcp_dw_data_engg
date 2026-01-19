CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refadjustmentcode`
AS SELECT
  `ecw_refadjustmentcode`.adjustmentcodekey,
  `ecw_refadjustmentcode`.adjcode,
  `ecw_refadjustmentcode`.adjname,
  `ecw_refadjustmentcode`.adjdescription,
  `ecw_refadjustmentcode`.adjustmentcategorykey,
  `ecw_refadjustmentcode`.dwlastupdatedatetime,
  `ecw_refadjustmentcode`.sourcesystemcode,
  `ecw_refadjustmentcode`.insertedby,
  `ecw_refadjustmentcode`.inserteddtm,
  `ecw_refadjustmentcode`.modifiedby,
  `ecw_refadjustmentcode`.modifieddtm,
  `ecw_refadjustmentcode`.nonparflag,
  `ecw_refadjustmentcode`.billableflag,
  `ecw_refadjustmentcode`.adjsubcategoryname,
  `ecw_refadjustmentcode`.sap101
  FROM
    edwpsc.`ecw_refadjustmentcode`
;