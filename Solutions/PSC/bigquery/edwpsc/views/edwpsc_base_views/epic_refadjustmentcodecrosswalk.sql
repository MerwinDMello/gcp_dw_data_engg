CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refadjustmentcodecrosswalk`
AS SELECT
  `epic_refadjustmentcodecrosswalk`.adjcode,
  `epic_refadjustmentcodecrosswalk`.regionkey,
  `epic_refadjustmentcodecrosswalk`.adjustmentcategorykey,
  `epic_refadjustmentcodecrosswalk`.insertedby,
  `epic_refadjustmentcodecrosswalk`.inserteddtm,
  `epic_refadjustmentcodecrosswalk`.modifiedby,
  `epic_refadjustmentcodecrosswalk`.modifieddtm
  FROM
    edwpsc.`epic_refadjustmentcodecrosswalk`
;