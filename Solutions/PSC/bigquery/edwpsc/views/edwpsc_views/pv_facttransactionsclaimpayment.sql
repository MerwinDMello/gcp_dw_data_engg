CREATE OR REPLACE VIEW edwpsc_views.`pv_facttransactionsclaimpayment`
AS SELECT
  `pv_facttransactionsclaimpayment`.transactionsclaimpaymentkey,
  `pv_facttransactionsclaimpayment`.claimkey,
  `pv_facttransactionsclaimpayment`.claimnumber,
  `pv_facttransactionsclaimpayment`.regionkey,
  `pv_facttransactionsclaimpayment`.coid,
  `pv_facttransactionsclaimpayment`.coidconfigurationkey,
  `pv_facttransactionsclaimpayment`.servicingproviderkey,
  `pv_facttransactionsclaimpayment`.claimpayer1iplankey,
  `pv_facttransactionsclaimpayment`.facilitykey,
  `pv_facttransactionsclaimpayment`.claimpaymentkey,
  `pv_facttransactionsclaimpayment`.transactiontype,
  `pv_facttransactionsclaimpayment`.transactionflag,
  `pv_facttransactionsclaimpayment`.transactionamt,
  `pv_facttransactionsclaimpayment`.transactiondatekey,
  `pv_facttransactionsclaimpayment`.transactiontime,
  `pv_facttransactionsclaimpayment`.transactionclosingdatekey,
  `pv_facttransactionsclaimpayment`.transactionbyuserkey,
  `pv_facttransactionsclaimpayment`.trrefid,
  `pv_facttransactionsclaimpayment`.practicekey,
  `pv_facttransactionsclaimpayment`.practicename,
  `pv_facttransactionsclaimpayment`.sourceaprimarykeyvalue,
  `pv_facttransactionsclaimpayment`.sourcearecordlastupdated,
  `pv_facttransactionsclaimpayment`.dwlastupdatedatetime,
  `pv_facttransactionsclaimpayment`.sourcesystemcode,
  `pv_facttransactionsclaimpayment`.insertedby,
  `pv_facttransactionsclaimpayment`.inserteddtm,
  `pv_facttransactionsclaimpayment`.modifiedby,
  `pv_facttransactionsclaimpayment`.modifieddtm,
  `pv_facttransactionsclaimpayment`.posteddatekey
  FROM
    edwpsc_base_views.`pv_facttransactionsclaimpayment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_facttransactionsclaimpayment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;