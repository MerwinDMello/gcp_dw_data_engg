CREATE OR REPLACE VIEW edwpsc_views.`epic_refadjustmentcode`
AS SELECT
  `epic_refadjustmentcode`.adjustmentcodekey,
  `epic_refadjustmentcode`.adjcode,
  `epic_refadjustmentcode`.adjname,
  `epic_refadjustmentcode`.adjdescription,
  `epic_refadjustmentcode`.adjshortname,
  `epic_refadjustmentcode`.adjactive,
  `epic_refadjustmentcode`.procid,
  `epic_refadjustmentcode`.regionkey,
  `epic_refadjustmentcode`.sourceaprimarykey,
  `epic_refadjustmentcode`.dwlastupdatedatetime,
  `epic_refadjustmentcode`.sourcesystemcode,
  `epic_refadjustmentcode`.insertedby,
  `epic_refadjustmentcode`.inserteddtm,
  `epic_refadjustmentcode`.modifiedby,
  `epic_refadjustmentcode`.modifieddtm,
  `epic_refadjustmentcode`.adjustmentcategorykey,
  `epic_refadjustmentcode`.nonparflag,
  `epic_refadjustmentcode`.billableflag,
  `epic_refadjustmentcode`.adjsubcategoryname
  FROM
    edwpsc_base_views.`epic_refadjustmentcode`
;