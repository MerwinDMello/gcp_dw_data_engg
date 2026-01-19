CREATE OR REPLACE VIEW edwpsc_views.`ecw_facttransactionsclaimlinepayment`
AS SELECT
  `ecw_facttransactionsclaimlinepayment`.transactionsclaimlinepaymentskey,
  `ecw_facttransactionsclaimlinepayment`.claimkey,
  `ecw_facttransactionsclaimlinepayment`.claimnumber,
  `ecw_facttransactionsclaimlinepayment`.regionkey,
  `ecw_facttransactionsclaimlinepayment`.coid,
  `ecw_facttransactionsclaimlinepayment`.coidconfigurationkey,
  `ecw_facttransactionsclaimlinepayment`.servicingproviderkey,
  `ecw_facttransactionsclaimlinepayment`.claimpayer1iplankey,
  `ecw_facttransactionsclaimlinepayment`.facilitykey,
  `ecw_facttransactionsclaimlinepayment`.claimlinepaymentskey,
  `ecw_facttransactionsclaimlinepayment`.transactiontype,
  `ecw_facttransactionsclaimlinepayment`.transactionflag,
  `ecw_facttransactionsclaimlinepayment`.transactionamt,
  `ecw_facttransactionsclaimlinepayment`.transactiondatekey,
  `ecw_facttransactionsclaimlinepayment`.transactiontime,
  `ecw_facttransactionsclaimlinepayment`.transactionbyuserkey,
  `ecw_facttransactionsclaimlinepayment`.sourceprimarykey,
  `ecw_facttransactionsclaimlinepayment`.sourcerecordlastupdated,
  `ecw_facttransactionsclaimlinepayment`.dwlastupdatedatetime,
  `ecw_facttransactionsclaimlinepayment`.sourcesystemcode,
  `ecw_facttransactionsclaimlinepayment`.insertedby,
  `ecw_facttransactionsclaimlinepayment`.inserteddtm,
  `ecw_facttransactionsclaimlinepayment`.modifiedby,
  `ecw_facttransactionsclaimlinepayment`.modifieddtm,
  `ecw_facttransactionsclaimlinepayment`.trrefid,
  `ecw_facttransactionsclaimlinepayment`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_facttransactionsclaimlinepayment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_facttransactionsclaimlinepayment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;