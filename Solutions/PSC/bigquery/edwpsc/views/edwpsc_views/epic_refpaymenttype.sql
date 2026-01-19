CREATE OR REPLACE VIEW edwpsc_views.`epic_refpaymenttype`
AS SELECT
  `epic_refpaymenttype`.paymenttypekey,
  `epic_refpaymenttype`.paymenttype,
  `epic_refpaymenttype`.paymenttypename,
  `epic_refpaymenttype`.paymenttypedescription,
  `epic_refpaymenttype`.paymenttypeshortname,
  `epic_refpaymenttype`.paymenttypeactive,
  `epic_refpaymenttype`.procid,
  `epic_refpaymenttype`.regionkey,
  `epic_refpaymenttype`.sourceaprimarykey,
  `epic_refpaymenttype`.dwlastupdatedatetime,
  `epic_refpaymenttype`.sourcesystemcode,
  `epic_refpaymenttype`.insertedby,
  `epic_refpaymenttype`.inserteddtm,
  `epic_refpaymenttype`.modifiedby,
  `epic_refpaymenttype`.modifieddtm
  FROM
    edwpsc_base_views.`epic_refpaymenttype`
;