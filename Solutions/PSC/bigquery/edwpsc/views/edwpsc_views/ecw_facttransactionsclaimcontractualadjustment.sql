CREATE OR REPLACE VIEW edwpsc_views.`ecw_facttransactionsclaimcontractualadjustment`
AS SELECT
  `ecw_facttransactionsclaimcontractualadjustment`.transactionsclaimcontractualadjustmentkey,
  `ecw_facttransactionsclaimcontractualadjustment`.claimkey,
  `ecw_facttransactionsclaimcontractualadjustment`.claimnumber,
  `ecw_facttransactionsclaimcontractualadjustment`.regionkey,
  `ecw_facttransactionsclaimcontractualadjustment`.coid,
  `ecw_facttransactionsclaimcontractualadjustment`.coidconfigurationkey,
  `ecw_facttransactionsclaimcontractualadjustment`.servicingproviderkey,
  `ecw_facttransactionsclaimcontractualadjustment`.claimpayer1iplankey,
  `ecw_facttransactionsclaimcontractualadjustment`.facilitykey,
  `ecw_facttransactionsclaimcontractualadjustment`.claimpaymentkey,
  `ecw_facttransactionsclaimcontractualadjustment`.transactiontype,
  `ecw_facttransactionsclaimcontractualadjustment`.transactionflag,
  `ecw_facttransactionsclaimcontractualadjustment`.transactionamt,
  `ecw_facttransactionsclaimcontractualadjustment`.transactiondatekey,
  `ecw_facttransactionsclaimcontractualadjustment`.transactiontime,
  `ecw_facttransactionsclaimcontractualadjustment`.transactionbyuserkey,
  `ecw_facttransactionsclaimcontractualadjustment`.sourceaprimarykeyvalue,
  `ecw_facttransactionsclaimcontractualadjustment`.sourcearecordlastupdated,
  `ecw_facttransactionsclaimcontractualadjustment`.dwlastupdatedatetime,
  `ecw_facttransactionsclaimcontractualadjustment`.sourcesystemcode,
  `ecw_facttransactionsclaimcontractualadjustment`.insertedby,
  `ecw_facttransactionsclaimcontractualadjustment`.inserteddtm,
  `ecw_facttransactionsclaimcontractualadjustment`.modifiedby,
  `ecw_facttransactionsclaimcontractualadjustment`.modifieddtm,
  `ecw_facttransactionsclaimcontractualadjustment`.trrefid,
  `ecw_facttransactionsclaimcontractualadjustment`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_facttransactionsclaimcontractualadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_facttransactionsclaimcontractualadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;