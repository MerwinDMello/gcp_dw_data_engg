CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refpayerresponseadjustmentcodecrosswalk`
AS SELECT
  `ecw_refpayerresponseadjustmentcodecrosswalk`.adjcode,
  `ecw_refpayerresponseadjustmentcodecrosswalk`.denialcategory,
  `ecw_refpayerresponseadjustmentcodecrosswalk`.insertedby,
  `ecw_refpayerresponseadjustmentcodecrosswalk`.inserteddtm,
  `ecw_refpayerresponseadjustmentcodecrosswalk`.modifiedby,
  `ecw_refpayerresponseadjustmentcodecrosswalk`.modifieddtm,
  `ecw_refpayerresponseadjustmentcodecrosswalk`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_refpayerresponseadjustmentcodecrosswalk`
;