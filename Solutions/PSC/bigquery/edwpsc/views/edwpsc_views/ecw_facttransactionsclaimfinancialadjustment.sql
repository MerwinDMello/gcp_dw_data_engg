CREATE OR REPLACE VIEW edwpsc_views.`ecw_facttransactionsclaimfinancialadjustment`
AS SELECT
  `ecw_facttransactionsclaimfinancialadjustment`.transactionsclaimfinancialadjustmentkey,
  `ecw_facttransactionsclaimfinancialadjustment`.claimkey,
  `ecw_facttransactionsclaimfinancialadjustment`.claimnumber,
  `ecw_facttransactionsclaimfinancialadjustment`.regionkey,
  `ecw_facttransactionsclaimfinancialadjustment`.coid,
  `ecw_facttransactionsclaimfinancialadjustment`.coidconfigurationkey,
  `ecw_facttransactionsclaimfinancialadjustment`.servicingproviderkey,
  `ecw_facttransactionsclaimfinancialadjustment`.claimpayer1iplankey,
  `ecw_facttransactionsclaimfinancialadjustment`.facilitykey,
  `ecw_facttransactionsclaimfinancialadjustment`.claimfinancialadjustmentkey,
  `ecw_facttransactionsclaimfinancialadjustment`.adjustmentcodekey,
  `ecw_facttransactionsclaimfinancialadjustment`.transactiontype,
  `ecw_facttransactionsclaimfinancialadjustment`.transactionflag,
  `ecw_facttransactionsclaimfinancialadjustment`.transactionamt,
  `ecw_facttransactionsclaimfinancialadjustment`.transactiondatekey,
  `ecw_facttransactionsclaimfinancialadjustment`.transactiontime,
  `ecw_facttransactionsclaimfinancialadjustment`.transactionbyuserkey,
  `ecw_facttransactionsclaimfinancialadjustment`.sourceaprimarykeyvalue,
  `ecw_facttransactionsclaimfinancialadjustment`.sourcearecordlastupdated,
  `ecw_facttransactionsclaimfinancialadjustment`.dwlastupdatedatetime,
  `ecw_facttransactionsclaimfinancialadjustment`.sourcesystemcode,
  `ecw_facttransactionsclaimfinancialadjustment`.insertedby,
  `ecw_facttransactionsclaimfinancialadjustment`.inserteddtm,
  `ecw_facttransactionsclaimfinancialadjustment`.modifiedby,
  `ecw_facttransactionsclaimfinancialadjustment`.modifieddtm,
  `ecw_facttransactionsclaimfinancialadjustment`.trrefid,
  `ecw_facttransactionsclaimfinancialadjustment`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_facttransactionsclaimfinancialadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_facttransactionsclaimfinancialadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;