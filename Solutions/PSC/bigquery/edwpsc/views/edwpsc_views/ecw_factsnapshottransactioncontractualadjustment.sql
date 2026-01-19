CREATE OR REPLACE VIEW edwpsc_views.`ecw_factsnapshottransactioncontractualadjustment`
AS SELECT
  `ecw_factsnapshottransactioncontractualadjustment`.transactioncontractualadjustmentkey,
  `ecw_factsnapshottransactioncontractualadjustment`.monthid,
  `ecw_factsnapshottransactioncontractualadjustment`.snapshotdate,
  `ecw_factsnapshottransactioncontractualadjustment`.regionkey,
  `ecw_factsnapshottransactioncontractualadjustment`.coid,
  `ecw_factsnapshottransactioncontractualadjustment`.gldepartment,
  `ecw_factsnapshottransactioncontractualadjustment`.claimkey,
  `ecw_factsnapshottransactioncontractualadjustment`.claimnumber,
  `ecw_factsnapshottransactioncontractualadjustment`.patientkey,
  `ecw_factsnapshottransactioncontractualadjustment`.patientid,
  `ecw_factsnapshottransactioncontractualadjustment`.servicingproviderkey,
  `ecw_factsnapshottransactioncontractualadjustment`.servicingproviderid,
  `ecw_factsnapshottransactioncontractualadjustment`.renderingproviderkey,
  `ecw_factsnapshottransactioncontractualadjustment`.renderingproviderid,
  `ecw_factsnapshottransactioncontractualadjustment`.facilitykey,
  `ecw_factsnapshottransactioncontractualadjustment`.facilityid,
  `ecw_factsnapshottransactioncontractualadjustment`.claimdatekey,
  `ecw_factsnapshottransactioncontractualadjustment`.claimdatemonthid,
  `ecw_factsnapshottransactioncontractualadjustment`.servicedatekey,
  `ecw_factsnapshottransactioncontractualadjustment`.servicedatemonthid,
  `ecw_factsnapshottransactioncontractualadjustment`.iplan1iplankey,
  `ecw_factsnapshottransactioncontractualadjustment`.iplan1id,
  `ecw_factsnapshottransactioncontractualadjustment`.financialclasskey,
  `ecw_factsnapshottransactioncontractualadjustment`.transactionid,
  `ecw_factsnapshottransactioncontractualadjustment`.transactionamt,
  `ecw_factsnapshottransactioncontractualadjustment`.transactiondatekey,
  `ecw_factsnapshottransactioncontractualadjustment`.transactiondatemonthid,
  `ecw_factsnapshottransactioncontractualadjustment`.dwlastupdatedatetime,
  `ecw_factsnapshottransactioncontractualadjustment`.sourcesystemcode,
  `ecw_factsnapshottransactioncontractualadjustment`.insertedby,
  `ecw_factsnapshottransactioncontractualadjustment`.inserteddtm,
  `ecw_factsnapshottransactioncontractualadjustment`.modifiedby,
  `ecw_factsnapshottransactioncontractualadjustment`.modifieddtm,
  `ecw_factsnapshottransactioncontractualadjustment`.transactiontype,
  `ecw_factsnapshottransactioncontractualadjustment`.paymentfromiplanid,
  `ecw_factsnapshottransactioncontractualadjustment`.paymentdetailid,
  `ecw_factsnapshottransactioncontractualadjustment`.transactionsamemonthflag
  FROM
    edwpsc_base_views.`ecw_factsnapshottransactioncontractualadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factsnapshottransactioncontractualadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;