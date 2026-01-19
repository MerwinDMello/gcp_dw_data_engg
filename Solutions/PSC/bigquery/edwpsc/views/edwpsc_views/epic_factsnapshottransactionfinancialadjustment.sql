CREATE OR REPLACE VIEW edwpsc_views.`epic_factsnapshottransactionfinancialadjustment`
AS SELECT
  `epic_factsnapshottransactionfinancialadjustment`.transactionfinancialadjustmentkey,
  `epic_factsnapshottransactionfinancialadjustment`.monthid,
  `epic_factsnapshottransactionfinancialadjustment`.snapshotdate,
  `epic_factsnapshottransactionfinancialadjustment`.regionkey,
  `epic_factsnapshottransactionfinancialadjustment`.practicekey,
  `epic_factsnapshottransactionfinancialadjustment`.practiceid,
  `epic_factsnapshottransactionfinancialadjustment`.coid,
  `epic_factsnapshottransactionfinancialadjustment`.gldepartment,
  `epic_factsnapshottransactionfinancialadjustment`.claimkey,
  `epic_factsnapshottransactionfinancialadjustment`.claimnumber,
  `epic_factsnapshottransactionfinancialadjustment`.visitnumber,
  `epic_factsnapshottransactionfinancialadjustment`.transactionnumber,
  `epic_factsnapshottransactionfinancialadjustment`.patientkey,
  `epic_factsnapshottransactionfinancialadjustment`.patientid,
  `epic_factsnapshottransactionfinancialadjustment`.servicingproviderkey,
  `epic_factsnapshottransactionfinancialadjustment`.servicingproviderid,
  `epic_factsnapshottransactionfinancialadjustment`.renderingproviderkey,
  `epic_factsnapshottransactionfinancialadjustment`.renderingproviderid,
  `epic_factsnapshottransactionfinancialadjustment`.facilitykey,
  `epic_factsnapshottransactionfinancialadjustment`.facilityid,
  `epic_factsnapshottransactionfinancialadjustment`.claimdatekey,
  `epic_factsnapshottransactionfinancialadjustment`.claimdatemonthid,
  `epic_factsnapshottransactionfinancialadjustment`.servicedatekey,
  `epic_factsnapshottransactionfinancialadjustment`.servicedatemonthid,
  `epic_factsnapshottransactionfinancialadjustment`.iplan1iplankey,
  `epic_factsnapshottransactionfinancialadjustment`.iplan1id,
  `epic_factsnapshottransactionfinancialadjustment`.financialclasskey,
  `epic_factsnapshottransactionfinancialadjustment`.transactionid,
  `epic_factsnapshottransactionfinancialadjustment`.transactionamt,
  `epic_factsnapshottransactionfinancialadjustment`.transactiondatekey,
  `epic_factsnapshottransactionfinancialadjustment`.transactiondatemonthid,
  `epic_factsnapshottransactionfinancialadjustment`.adjustmentcodekey,
  `epic_factsnapshottransactionfinancialadjustment`.adjustmentcode,
  `epic_factsnapshottransactionfinancialadjustment`.transactiontype,
  `epic_factsnapshottransactionfinancialadjustment`.adjustmentid,
  `epic_factsnapshottransactionfinancialadjustment`.dwlastupdatedatetime,
  `epic_factsnapshottransactionfinancialadjustment`.sourcesystemcode,
  `epic_factsnapshottransactionfinancialadjustment`.insertedby,
  `epic_factsnapshottransactionfinancialadjustment`.inserteddtm,
  `epic_factsnapshottransactionfinancialadjustment`.transactionsamemonthflag
  FROM
    edwpsc_base_views.`epic_factsnapshottransactionfinancialadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factsnapshottransactionfinancialadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;