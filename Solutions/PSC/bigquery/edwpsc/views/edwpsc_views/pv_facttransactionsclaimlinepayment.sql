CREATE OR REPLACE VIEW edwpsc_views.`pv_facttransactionsclaimlinepayment`
AS SELECT
  `pv_facttransactionsclaimlinepayment`.transactionsclaimlinepaymentkey,
  `pv_facttransactionsclaimlinepayment`.claimkey,
  `pv_facttransactionsclaimlinepayment`.claimnumber,
  `pv_facttransactionsclaimlinepayment`.regionkey,
  `pv_facttransactionsclaimlinepayment`.coid,
  `pv_facttransactionsclaimlinepayment`.coidconfigurationkey,
  `pv_facttransactionsclaimlinepayment`.servicingproviderkey,
  `pv_facttransactionsclaimlinepayment`.claimpayer1iplankey,
  `pv_facttransactionsclaimlinepayment`.facilitykey,
  `pv_facttransactionsclaimlinepayment`.claimlinepaymentskey,
  `pv_facttransactionsclaimlinepayment`.transactiontype,
  `pv_facttransactionsclaimlinepayment`.transactionflag,
  `pv_facttransactionsclaimlinepayment`.transactionamt,
  `pv_facttransactionsclaimlinepayment`.transactiondatekey,
  `pv_facttransactionsclaimlinepayment`.transactiontime,
  `pv_facttransactionsclaimlinepayment`.transactionclosingdatekey,
  `pv_facttransactionsclaimlinepayment`.transactionbyuserkey,
  `pv_facttransactionsclaimlinepayment`.trrefid,
  `pv_facttransactionsclaimlinepayment`.practicekey,
  `pv_facttransactionsclaimlinepayment`.practicename,
  `pv_facttransactionsclaimlinepayment`.sourceaprimarykeyvalue,
  `pv_facttransactionsclaimlinepayment`.sourcearecordlastupdated,
  `pv_facttransactionsclaimlinepayment`.dwlastupdatedatetime,
  `pv_facttransactionsclaimlinepayment`.sourcesystemcode,
  `pv_facttransactionsclaimlinepayment`.insertedby,
  `pv_facttransactionsclaimlinepayment`.inserteddtm,
  `pv_facttransactionsclaimlinepayment`.modifiedby,
  `pv_facttransactionsclaimlinepayment`.modifieddtm,
  `pv_facttransactionsclaimlinepayment`.posteddatekey
  FROM
    edwpsc_base_views.`pv_facttransactionsclaimlinepayment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_facttransactionsclaimlinepayment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;