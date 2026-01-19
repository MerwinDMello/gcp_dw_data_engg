CREATE OR REPLACE VIEW edwpsc_views.`pv_facttransactionsclaimlinecharge`
AS SELECT
  `pv_facttransactionsclaimlinecharge`.transactionsclaimlinechargekey,
  `pv_facttransactionsclaimlinecharge`.claimkey,
  `pv_facttransactionsclaimlinecharge`.claimnumber,
  `pv_facttransactionsclaimlinecharge`.regionkey,
  `pv_facttransactionsclaimlinecharge`.coid,
  `pv_facttransactionsclaimlinecharge`.coidconfigurationkey,
  `pv_facttransactionsclaimlinecharge`.servicingproviderkey,
  `pv_facttransactionsclaimlinecharge`.claimpayer1iplankey,
  `pv_facttransactionsclaimlinecharge`.facilitykey,
  `pv_facttransactionsclaimlinecharge`.claimlinechargekey,
  `pv_facttransactionsclaimlinecharge`.cptcodekey,
  `pv_facttransactionsclaimlinecharge`.transactiontype,
  `pv_facttransactionsclaimlinecharge`.transactionflag,
  `pv_facttransactionsclaimlinecharge`.transactionamt,
  `pv_facttransactionsclaimlinecharge`.transactiondatekey,
  `pv_facttransactionsclaimlinecharge`.transactiontime,
  `pv_facttransactionsclaimlinecharge`.transactionclosingdatekey,
  `pv_facttransactionsclaimlinecharge`.transactionbyuserkey,
  `pv_facttransactionsclaimlinecharge`.sourceaprimarykeyvalue,
  `pv_facttransactionsclaimlinecharge`.sourcearecordlastupdated,
  `pv_facttransactionsclaimlinecharge`.dwlastupdatedatetime,
  `pv_facttransactionsclaimlinecharge`.sourcesystemcode,
  `pv_facttransactionsclaimlinecharge`.insertedby,
  `pv_facttransactionsclaimlinecharge`.inserteddtm,
  `pv_facttransactionsclaimlinecharge`.modifiedby,
  `pv_facttransactionsclaimlinecharge`.modifieddtm,
  `pv_facttransactionsclaimlinecharge`.trrefid,
  `pv_facttransactionsclaimlinecharge`.practicekey,
  `pv_facttransactionsclaimlinecharge`.practicename
  FROM
    edwpsc_base_views.`pv_facttransactionsclaimlinecharge`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_facttransactionsclaimlinecharge`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;