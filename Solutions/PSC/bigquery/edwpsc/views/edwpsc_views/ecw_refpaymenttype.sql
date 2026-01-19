CREATE OR REPLACE VIEW edwpsc_views.`ecw_refpaymenttype`
AS SELECT
  `ecw_refpaymenttype`.paymenttypekey,
  `ecw_refpaymenttype`.paymenttypename,
  `ecw_refpaymenttype`.dwlastupdatedatetime,
  `ecw_refpaymenttype`.sourcesystemcode,
  `ecw_refpaymenttype`.insertedby,
  `ecw_refpaymenttype`.inserteddtm,
  `ecw_refpaymenttype`.modifiedby,
  `ecw_refpaymenttype`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refpaymenttype`
;