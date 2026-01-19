CREATE OR REPLACE VIEW edwpsc_views.`epic_facttransactionsclaimcontractualadjustment`
AS SELECT
  `epic_facttransactionsclaimcontractualadjustment`.transactionsclaimcontractualadjustmentkey,
  `epic_facttransactionsclaimcontractualadjustment`.claimkey,
  `epic_facttransactionsclaimcontractualadjustment`.claimnumber,
  `epic_facttransactionsclaimcontractualadjustment`.transactionnumber,
  `epic_facttransactionsclaimcontractualadjustment`.visitnumber,
  `epic_facttransactionsclaimcontractualadjustment`.regionkey,
  `epic_facttransactionsclaimcontractualadjustment`.coid,
  `epic_facttransactionsclaimcontractualadjustment`.coidconfigurationkey,
  `epic_facttransactionsclaimcontractualadjustment`.servicingproviderkey,
  `epic_facttransactionsclaimcontractualadjustment`.claimpayer1iplankey,
  `epic_facttransactionsclaimcontractualadjustment`.claimlineliabilityiplankey,
  `epic_facttransactionsclaimcontractualadjustment`.facilitykey,
  `epic_facttransactionsclaimcontractualadjustment`.claimpaymentkey,
  `epic_facttransactionsclaimcontractualadjustment`.adjustmentcodekey,
  `epic_facttransactionsclaimcontractualadjustment`.trrefid,
  `epic_facttransactionsclaimcontractualadjustment`.transactiontype,
  `epic_facttransactionsclaimcontractualadjustment`.transactionflag,
  `epic_facttransactionsclaimcontractualadjustment`.transactionamt,
  `epic_facttransactionsclaimcontractualadjustment`.transactiondatekey,
  `epic_facttransactionsclaimcontractualadjustment`.transactiontime,
  `epic_facttransactionsclaimcontractualadjustment`.transactionbyuserkey,
  `epic_facttransactionsclaimcontractualadjustment`.transactionperiod,
  `epic_facttransactionsclaimcontractualadjustment`.sourceaprimarykeyvalue,
  `epic_facttransactionsclaimcontractualadjustment`.sourcearecordlastupdated,
  `epic_facttransactionsclaimcontractualadjustment`.sourcebprimarykeyvalue,
  `epic_facttransactionsclaimcontractualadjustment`.sourcebrecordlastupdated,
  `epic_facttransactionsclaimcontractualadjustment`.dwlastupdatedatetime,
  `epic_facttransactionsclaimcontractualadjustment`.sourcesystemcode,
  `epic_facttransactionsclaimcontractualadjustment`.insertedby,
  `epic_facttransactionsclaimcontractualadjustment`.inserteddtm,
  `epic_facttransactionsclaimcontractualadjustment`.modifiedby,
  `epic_facttransactionsclaimcontractualadjustment`.modifieddtm,
  `epic_facttransactionsclaimcontractualadjustment`.matchingtransactionnumber,
  `epic_facttransactionsclaimcontractualadjustment`.debitcreditflag,
  `epic_facttransactionsclaimcontractualadjustment`.matchingtrrefid,
  `epic_facttransactionsclaimcontractualadjustment`.accountkey,
  `epic_facttransactionsclaimcontractualadjustment`.patientkey,
  `epic_facttransactionsclaimcontractualadjustment`.distributedbyuserkey,
  `epic_facttransactionsclaimcontractualadjustment`.undistributedbyuserkey,
  `epic_facttransactionsclaimcontractualadjustment`.etrid,
  `epic_facttransactionsclaimcontractualadjustment`.matchingetrid
  FROM
    edwpsc_base_views.`epic_facttransactionsclaimcontractualadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_facttransactionsclaimcontractualadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;