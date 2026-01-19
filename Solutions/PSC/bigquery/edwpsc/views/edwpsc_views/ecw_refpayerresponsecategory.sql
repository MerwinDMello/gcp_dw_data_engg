CREATE OR REPLACE VIEW edwpsc_views.`ecw_refpayerresponsecategory`
AS SELECT
  `ecw_refpayerresponsecategory`.denialcategory,
  `ecw_refpayerresponsecategory`.payerresponsecategory,
  `ecw_refpayerresponsecategory`.insertedby,
  `ecw_refpayerresponsecategory`.inserteddtm,
  `ecw_refpayerresponsecategory`.modifiedby,
  `ecw_refpayerresponsecategory`.modifieddtm,
  `ecw_refpayerresponsecategory`.priorityrank,
  `ecw_refpayerresponsecategory`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_refpayerresponsecategory`
;