CREATE OR REPLACE VIEW edwpsc_views.`ecw_facttransactionspaymentheader`
AS SELECT
  `ecw_facttransactionspaymentheader`.transactionpaymentheaderkey,
  `ecw_facttransactionspaymentheader`.regionkey,
  `ecw_facttransactionspaymentheader`.paymentheaderdate,
  `ecw_facttransactionspaymentheader`.paymentheadertime,
  `ecw_facttransactionspaymentheader`.paymentheaderuserkey,
  `ecw_facttransactionspaymentheader`.paymentheaderuserid,
  `ecw_facttransactionspaymentheader`.transactionflag,
  `ecw_facttransactionspaymentheader`.paymentid,
  `ecw_facttransactionspaymentheader`.paymentheaderamt,
  `ecw_facttransactionspaymentheader`.paymentheaderdescription,
  `ecw_facttransactionspaymentheader`.paymentheadermodifieddate,
  `ecw_facttransactionspaymentheader`.sourceaprimarykeyvalue,
  `ecw_facttransactionspaymentheader`.dwlastupdatedatetime,
  `ecw_facttransactionspaymentheader`.sourcesystemcode,
  `ecw_facttransactionspaymentheader`.insertedby,
  `ecw_facttransactionspaymentheader`.inserteddtm,
  `ecw_facttransactionspaymentheader`.modifiedby,
  `ecw_facttransactionspaymentheader`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_facttransactionspaymentheader`
;