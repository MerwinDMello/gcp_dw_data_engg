CREATE OR REPLACE VIEW edwpsc_views.`ecw_facttransactionsclaimpayment`
AS SELECT
  `ecw_facttransactionsclaimpayment`.transactionsclaimpaymentkey,
  `ecw_facttransactionsclaimpayment`.claimkey,
  `ecw_facttransactionsclaimpayment`.claimnumber,
  `ecw_facttransactionsclaimpayment`.regionkey,
  `ecw_facttransactionsclaimpayment`.coid,
  `ecw_facttransactionsclaimpayment`.coidconfigurationkey,
  `ecw_facttransactionsclaimpayment`.servicingproviderkey,
  `ecw_facttransactionsclaimpayment`.claimpayer1iplankey,
  `ecw_facttransactionsclaimpayment`.facilitykey,
  `ecw_facttransactionsclaimpayment`.claimpaymentkey,
  `ecw_facttransactionsclaimpayment`.transactiontype,
  `ecw_facttransactionsclaimpayment`.transactionflag,
  `ecw_facttransactionsclaimpayment`.transactionamt,
  `ecw_facttransactionsclaimpayment`.transactiondatekey,
  `ecw_facttransactionsclaimpayment`.transactiontime,
  `ecw_facttransactionsclaimpayment`.transactionbyuserkey,
  `ecw_facttransactionsclaimpayment`.sourceaprimarykeyvalue,
  `ecw_facttransactionsclaimpayment`.sourcearecordlastupdated,
  `ecw_facttransactionsclaimpayment`.dwlastupdatedatetime,
  `ecw_facttransactionsclaimpayment`.sourcesystemcode,
  `ecw_facttransactionsclaimpayment`.insertedby,
  `ecw_facttransactionsclaimpayment`.inserteddtm,
  `ecw_facttransactionsclaimpayment`.modifiedby,
  `ecw_facttransactionsclaimpayment`.modifieddtm,
  `ecw_facttransactionsclaimpayment`.trrefid,
  `ecw_facttransactionsclaimpayment`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_facttransactionsclaimpayment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_facttransactionsclaimpayment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;