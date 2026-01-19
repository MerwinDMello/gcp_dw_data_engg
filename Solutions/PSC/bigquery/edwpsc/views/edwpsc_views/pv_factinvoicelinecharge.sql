CREATE OR REPLACE VIEW edwpsc_views.`pv_factinvoicelinecharge`
AS SELECT
  `pv_factinvoicelinecharge`.invoicelinechargekey,
  `pv_factinvoicelinecharge`.regionkey,
  `pv_factinvoicelinecharge`.coid,
  `pv_factinvoicelinecharge`.practice,
  `pv_factinvoicelinecharge`.invoicekey,
  `pv_factinvoicelinecharge`.invoicenumber,
  `pv_factinvoicelinecharge`.invoicelinenumber,
  `pv_factinvoicelinecharge`.servicedatekey,
  `pv_factinvoicelinecharge`.invoicedescription,
  `pv_factinvoicelinecharge`.invoiceunits,
  `pv_factinvoicelinecharge`.linechargesperunit,
  `pv_factinvoicelinecharge`.linechargesamt,
  `pv_factinvoicelinecharge`.linepaymentamt,
  `pv_factinvoicelinecharge`.lineadjustmentamt,
  `pv_factinvoicelinecharge`.linebalanceamt,
  `pv_factinvoicelinecharge`.linecalculatedbalance,
  `pv_factinvoicelinecharge`.transactiondatekey,
  `pv_factinvoicelinecharge`.lastupdatedbyuserkey,
  `pv_factinvoicelinecharge`.deleteflag,
  `pv_factinvoicelinecharge`.sourceaprimarykeyvalue,
  `pv_factinvoicelinecharge`.sourcearecordlastupdated,
  `pv_factinvoicelinecharge`.sourcebprimarykeyvalue,
  `pv_factinvoicelinecharge`.sourcebrecordlastupdated,
  `pv_factinvoicelinecharge`.dwlastupdatedatetime,
  `pv_factinvoicelinecharge`.sourcesystemcode,
  `pv_factinvoicelinecharge`.insertedby,
  `pv_factinvoicelinecharge`.inserteddtm,
  `pv_factinvoicelinecharge`.modifiedby,
  `pv_factinvoicelinecharge`.modifieddtm
  FROM
    edwpsc_base_views.`pv_factinvoicelinecharge`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factinvoicelinecharge`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;