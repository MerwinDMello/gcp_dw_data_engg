CREATE OR REPLACE VIEW edwpsc_views.`ecw_facttransactionsclaimlinecharge`
AS SELECT
  `ecw_facttransactionsclaimlinecharge`.transactionsclaimlinechargekey,
  `ecw_facttransactionsclaimlinecharge`.claimkey,
  `ecw_facttransactionsclaimlinecharge`.claimnumber,
  `ecw_facttransactionsclaimlinecharge`.regionkey,
  `ecw_facttransactionsclaimlinecharge`.coid,
  `ecw_facttransactionsclaimlinecharge`.coidconfigurationkey,
  `ecw_facttransactionsclaimlinecharge`.servicingproviderkey,
  `ecw_facttransactionsclaimlinecharge`.claimpayer1iplankey,
  `ecw_facttransactionsclaimlinecharge`.facilitykey,
  `ecw_facttransactionsclaimlinecharge`.claimlinechargekey,
  `ecw_facttransactionsclaimlinecharge`.cptcodekey,
  `ecw_facttransactionsclaimlinecharge`.transactiontype,
  `ecw_facttransactionsclaimlinecharge`.transactionflag,
  `ecw_facttransactionsclaimlinecharge`.transactionamt,
  `ecw_facttransactionsclaimlinecharge`.transactiondatekey,
  `ecw_facttransactionsclaimlinecharge`.transactiontime,
  `ecw_facttransactionsclaimlinecharge`.transactionbyuserkey,
  `ecw_facttransactionsclaimlinecharge`.sourceaprimarykeyvalue,
  `ecw_facttransactionsclaimlinecharge`.sourcearecordlastupdated,
  `ecw_facttransactionsclaimlinecharge`.dwlastupdatedatetime,
  `ecw_facttransactionsclaimlinecharge`.sourcesystemcode,
  `ecw_facttransactionsclaimlinecharge`.insertedby,
  `ecw_facttransactionsclaimlinecharge`.inserteddtm,
  `ecw_facttransactionsclaimlinecharge`.modifiedby,
  `ecw_facttransactionsclaimlinecharge`.modifieddtm,
  `ecw_facttransactionsclaimlinecharge`.trrefid,
  `ecw_facttransactionsclaimlinecharge`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_facttransactionsclaimlinecharge`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_facttransactionsclaimlinecharge`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;