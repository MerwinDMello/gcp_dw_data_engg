CREATE OR REPLACE VIEW edwpsc_views.`epic_facttransactionsclaimlinecontractualadjustment`
AS SELECT
  `epic_facttransactionsclaimlinecontractualadjustment`.transactionsclaimlinecontractualadjustmentkey,
  `epic_facttransactionsclaimlinecontractualadjustment`.claimkey,
  `epic_facttransactionsclaimlinecontractualadjustment`.claimnumber,
  `epic_facttransactionsclaimlinecontractualadjustment`.transactionnumber,
  `epic_facttransactionsclaimlinecontractualadjustment`.visitnumber,
  `epic_facttransactionsclaimlinecontractualadjustment`.regionkey,
  `epic_facttransactionsclaimlinecontractualadjustment`.coid,
  `epic_facttransactionsclaimlinecontractualadjustment`.coidconfigurationkey,
  `epic_facttransactionsclaimlinecontractualadjustment`.servicingproviderkey,
  `epic_facttransactionsclaimlinecontractualadjustment`.claimpayer1iplankey,
  `epic_facttransactionsclaimlinecontractualadjustment`.claimlineliabilityiplankey,
  `epic_facttransactionsclaimlinecontractualadjustment`.facilitykey,
  `epic_facttransactionsclaimlinecontractualadjustment`.claimlinepaymentkey,
  `epic_facttransactionsclaimlinecontractualadjustment`.adjustmentcodekey,
  `epic_facttransactionsclaimlinecontractualadjustment`.trrefid,
  `epic_facttransactionsclaimlinecontractualadjustment`.transactiontype,
  `epic_facttransactionsclaimlinecontractualadjustment`.transactionflag,
  `epic_facttransactionsclaimlinecontractualadjustment`.transactionamt,
  `epic_facttransactionsclaimlinecontractualadjustment`.transactiondatekey,
  `epic_facttransactionsclaimlinecontractualadjustment`.transactiontime,
  `epic_facttransactionsclaimlinecontractualadjustment`.transactionbyuserkey,
  `epic_facttransactionsclaimlinecontractualadjustment`.transactionperiod,
  `epic_facttransactionsclaimlinecontractualadjustment`.sourceaprimarykeyvalue,
  `epic_facttransactionsclaimlinecontractualadjustment`.sourcearecordlastupdated,
  `epic_facttransactionsclaimlinecontractualadjustment`.sourcebprimarykeyvalue,
  `epic_facttransactionsclaimlinecontractualadjustment`.sourcebrecordlastupdated,
  `epic_facttransactionsclaimlinecontractualadjustment`.dwlastupdatedatetime,
  `epic_facttransactionsclaimlinecontractualadjustment`.sourcesystemcode,
  `epic_facttransactionsclaimlinecontractualadjustment`.insertedby,
  `epic_facttransactionsclaimlinecontractualadjustment`.inserteddtm,
  `epic_facttransactionsclaimlinecontractualadjustment`.modifiedby,
  `epic_facttransactionsclaimlinecontractualadjustment`.modifieddtm,
  `epic_facttransactionsclaimlinecontractualadjustment`.debitcreditflag,
  `epic_facttransactionsclaimlinecontractualadjustment`.matchingtrrefid,
  `epic_facttransactionsclaimlinecontractualadjustment`.accountkey,
  `epic_facttransactionsclaimlinecontractualadjustment`.patientkey,
  `epic_facttransactionsclaimlinecontractualadjustment`.distributedbyuserkey,
  `epic_facttransactionsclaimlinecontractualadjustment`.undistributedbyuserkey,
  `epic_facttransactionsclaimlinecontractualadjustment`.etrid,
  `epic_facttransactionsclaimlinecontractualadjustment`.matchingetrid
  FROM
    edwpsc_base_views.`epic_facttransactionsclaimlinecontractualadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_facttransactionsclaimlinecontractualadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;