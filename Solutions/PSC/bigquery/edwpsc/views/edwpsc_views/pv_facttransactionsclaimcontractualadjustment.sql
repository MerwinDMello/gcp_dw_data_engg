CREATE OR REPLACE VIEW edwpsc_views.`pv_facttransactionsclaimcontractualadjustment`
AS SELECT
  `pv_facttransactionsclaimcontractualadjustment`.transactionsclaimcontractualadjustmentkey,
  `pv_facttransactionsclaimcontractualadjustment`.claimkey,
  `pv_facttransactionsclaimcontractualadjustment`.claimnumber,
  `pv_facttransactionsclaimcontractualadjustment`.regionkey,
  `pv_facttransactionsclaimcontractualadjustment`.coid,
  `pv_facttransactionsclaimcontractualadjustment`.coidconfigurationkey,
  `pv_facttransactionsclaimcontractualadjustment`.servicingproviderkey,
  `pv_facttransactionsclaimcontractualadjustment`.claimpayer1iplankey,
  `pv_facttransactionsclaimcontractualadjustment`.facilitykey,
  `pv_facttransactionsclaimcontractualadjustment`.claimpaymentkey,
  `pv_facttransactionsclaimcontractualadjustment`.transactiontype,
  `pv_facttransactionsclaimcontractualadjustment`.transactionflag,
  `pv_facttransactionsclaimcontractualadjustment`.transactionamt,
  `pv_facttransactionsclaimcontractualadjustment`.transactiondatekey,
  `pv_facttransactionsclaimcontractualadjustment`.transactiontime,
  `pv_facttransactionsclaimcontractualadjustment`.transactionclosingdatekey,
  `pv_facttransactionsclaimcontractualadjustment`.transactionbyuserkey,
  `pv_facttransactionsclaimcontractualadjustment`.trrefid,
  `pv_facttransactionsclaimcontractualadjustment`.practicekey,
  `pv_facttransactionsclaimcontractualadjustment`.practicename,
  `pv_facttransactionsclaimcontractualadjustment`.sourceaprimarykeyvalue,
  `pv_facttransactionsclaimcontractualadjustment`.sourcearecordlastupdated,
  `pv_facttransactionsclaimcontractualadjustment`.dwlastupdatedatetime,
  `pv_facttransactionsclaimcontractualadjustment`.sourcesystemcode,
  `pv_facttransactionsclaimcontractualadjustment`.insertedby,
  `pv_facttransactionsclaimcontractualadjustment`.inserteddtm,
  `pv_facttransactionsclaimcontractualadjustment`.modifiedby,
  `pv_facttransactionsclaimcontractualadjustment`.modifieddtm,
  `pv_facttransactionsclaimcontractualadjustment`.posteddatekey
  FROM
    edwpsc_base_views.`pv_facttransactionsclaimcontractualadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_facttransactionsclaimcontractualadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;