CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refpaymenttypeepic`
AS SELECT
  `epic_refpaymenttypeepic`.epicpaymenttypekey,
  `epic_refpaymenttypeepic`.paymenttype,
  `epic_refpaymenttypeepic`.paymenttypename,
  `epic_refpaymenttypeepic`.paymenttypedescription,
  `epic_refpaymenttypeepic`.paymenttypeshortname,
  `epic_refpaymenttypeepic`.paymenttypeactive,
  `epic_refpaymenttypeepic`.procid,
  `epic_refpaymenttypeepic`.regionkey,
  `epic_refpaymenttypeepic`.sourceaprimarykey,
  `epic_refpaymenttypeepic`.dwlastupdatedatetime,
  `epic_refpaymenttypeepic`.sourcesystemcode,
  `epic_refpaymenttypeepic`.insertedby,
  `epic_refpaymenttypeepic`.inserteddtm,
  `epic_refpaymenttypeepic`.modifiedby,
  `epic_refpaymenttypeepic`.modifieddtm
  FROM
    edwpsc.`epic_refpaymenttypeepic`
;