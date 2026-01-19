CREATE OR REPLACE VIEW edwpsc_views.`epic_factsnapshottransactioncontractualadjustment`
AS SELECT
  `epic_factsnapshottransactioncontractualadjustment`.transactioncontractualadjustmentkey,
  `epic_factsnapshottransactioncontractualadjustment`.monthid,
  `epic_factsnapshottransactioncontractualadjustment`.snapshotdate,
  `epic_factsnapshottransactioncontractualadjustment`.regionkey,
  `epic_factsnapshottransactioncontractualadjustment`.practicekey,
  `epic_factsnapshottransactioncontractualadjustment`.practiceid,
  `epic_factsnapshottransactioncontractualadjustment`.coid,
  `epic_factsnapshottransactioncontractualadjustment`.gldepartment,
  `epic_factsnapshottransactioncontractualadjustment`.claimkey,
  `epic_factsnapshottransactioncontractualadjustment`.claimnumber,
  `epic_factsnapshottransactioncontractualadjustment`.visitnumber,
  `epic_factsnapshottransactioncontractualadjustment`.transactionnumber,
  `epic_factsnapshottransactioncontractualadjustment`.patientkey,
  `epic_factsnapshottransactioncontractualadjustment`.patientid,
  `epic_factsnapshottransactioncontractualadjustment`.servicingproviderkey,
  `epic_factsnapshottransactioncontractualadjustment`.servicingproviderid,
  `epic_factsnapshottransactioncontractualadjustment`.renderingproviderkey,
  `epic_factsnapshottransactioncontractualadjustment`.renderingproviderid,
  `epic_factsnapshottransactioncontractualadjustment`.facilitykey,
  `epic_factsnapshottransactioncontractualadjustment`.facilityid,
  `epic_factsnapshottransactioncontractualadjustment`.claimdatekey,
  `epic_factsnapshottransactioncontractualadjustment`.claimdatemonthid,
  `epic_factsnapshottransactioncontractualadjustment`.servicedatekey,
  `epic_factsnapshottransactioncontractualadjustment`.servicedatemonthid,
  `epic_factsnapshottransactioncontractualadjustment`.iplan1iplankey,
  `epic_factsnapshottransactioncontractualadjustment`.iplan1id,
  `epic_factsnapshottransactioncontractualadjustment`.financialclasskey,
  `epic_factsnapshottransactioncontractualadjustment`.transactionid,
  `epic_factsnapshottransactioncontractualadjustment`.transactionamt,
  `epic_factsnapshottransactioncontractualadjustment`.transactiondatekey,
  `epic_factsnapshottransactioncontractualadjustment`.transactiondatemonthid,
  `epic_factsnapshottransactioncontractualadjustment`.transactiontype,
  `epic_factsnapshottransactioncontractualadjustment`.paymentfromiplanid,
  `epic_factsnapshottransactioncontractualadjustment`.paymentdetailid,
  `epic_factsnapshottransactioncontractualadjustment`.dwlastupdatedatetime,
  `epic_factsnapshottransactioncontractualadjustment`.sourcesystemcode,
  `epic_factsnapshottransactioncontractualadjustment`.insertedby,
  `epic_factsnapshottransactioncontractualadjustment`.inserteddtm,
  `epic_factsnapshottransactioncontractualadjustment`.transactionsamemonthflag
  FROM
    edwpsc_base_views.`epic_factsnapshottransactioncontractualadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factsnapshottransactioncontractualadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;