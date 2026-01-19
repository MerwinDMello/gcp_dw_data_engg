CREATE OR REPLACE VIEW edwpsc_views.`pv_factinvoicelinepayment`
AS SELECT
  `pv_factinvoicelinepayment`.invoicelinepaymentkey,
  `pv_factinvoicelinepayment`.coid,
  `pv_factinvoicelinepayment`.regionkey,
  `pv_factinvoicelinepayment`.practice,
  `pv_factinvoicelinepayment`.invoicenumber,
  `pv_factinvoicelinepayment`.invoicelinenumber,
  `pv_factinvoicelinepayment`.invoicelinechargekey,
  `pv_factinvoicelinepayment`.paymentamt,
  `pv_factinvoicelinepayment`.paymentdatekey,
  `pv_factinvoicelinepayment`.paymenttype,
  `pv_factinvoicelinepayment`.transactionnumber,
  `pv_factinvoicelinepayment`.invoicekey,
  `pv_factinvoicelinepayment`.batchkey,
  `pv_factinvoicelinepayment`.closingdatekey,
  `pv_factinvoicelinepayment`.createdbyuserkey,
  `pv_factinvoicelinepayment`.invoicetransactiondesc,
  `pv_factinvoicelinepayment`.invoicereason,
  `pv_factinvoicelinepayment`.checknumber,
  `pv_factinvoicelinepayment`.checkdatekey,
  `pv_factinvoicelinepayment`.checktypedesc,
  `pv_factinvoicelinepayment`.paymentmsgcode,
  `pv_factinvoicelinepayment`.depositdatekey,
  `pv_factinvoicelinepayment`.paymentnumber,
  `pv_factinvoicelinepayment`.treasurybatchnumber,
  `pv_factinvoicelinepayment`.treasurybatchdepositdate,
  `pv_factinvoicelinepayment`.treasurybatchpayername,
  `pv_factinvoicelinepayment`.deleteflag,
  `pv_factinvoicelinepayment`.sourceaprimarykeyvalue,
  `pv_factinvoicelinepayment`.sourcearecordlastupdated,
  `pv_factinvoicelinepayment`.sourcebprimarykeyvalue,
  `pv_factinvoicelinepayment`.sourcebrecordlastupdated,
  `pv_factinvoicelinepayment`.dwlastupdatedatetime,
  `pv_factinvoicelinepayment`.sourcesystemcode,
  `pv_factinvoicelinepayment`.insertedby,
  `pv_factinvoicelinepayment`.inserteddtm,
  `pv_factinvoicelinepayment`.modifiedby,
  `pv_factinvoicelinepayment`.modifieddtm
  FROM
    edwpsc_base_views.`pv_factinvoicelinepayment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factinvoicelinepayment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;