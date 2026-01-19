CREATE OR REPLACE VIEW edwpsc_views.`pv_factinvoice`
AS SELECT
  `pv_factinvoice`.invoicekey,
  `pv_factinvoice`.coid,
  `pv_factinvoice`.regionkey,
  `pv_factinvoice`.practice,
  `pv_factinvoice`.invoicenumber,
  `pv_factinvoice`.companynumber,
  `pv_factinvoice`.invoicedate,
  `pv_factinvoice`.invoicetype,
  `pv_factinvoice`.totalchargesamt,
  `pv_factinvoice`.totalpaymentamt,
  `pv_factinvoice`.totaladjustmentamt,
  `pv_factinvoice`.totalbalanceamt,
  `pv_factinvoice`.totalendingbalanceamt,
  `pv_factinvoice`.servicingproviderkey,
  `pv_factinvoice`.referringproviderkey,
  `pv_factinvoice`.companyiplankey,
  `pv_factinvoice`.lastupdatedbyuserkey,
  `pv_factinvoice`.closingdatekey,
  `pv_factinvoice`.internalnotes,
  `pv_factinvoice`.externalnotes,
  `pv_factinvoice`.deleteflag,
  `pv_factinvoice`.sourceaprimarykeyvalue,
  `pv_factinvoice`.sourcearecordlastupdated,
  `pv_factinvoice`.sourcebprimarykeyvalue,
  `pv_factinvoice`.sourcebrecordlastupdated,
  `pv_factinvoice`.dwlastupdatedatetime,
  `pv_factinvoice`.sourcesystemcode,
  `pv_factinvoice`.insertedby,
  `pv_factinvoice`.inserteddtm,
  `pv_factinvoice`.modifiedby,
  `pv_factinvoice`.modifieddtm
  FROM
    edwpsc_base_views.`pv_factinvoice`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factinvoice`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;