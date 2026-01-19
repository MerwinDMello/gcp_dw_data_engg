CREATE OR REPLACE VIEW edwpsc_views.`ecw_factsnapshottransactionfinancialadjustment`
AS SELECT
  `ecw_factsnapshottransactionfinancialadjustment`.transactionfinancialadjustmentkey,
  `ecw_factsnapshottransactionfinancialadjustment`.monthid,
  `ecw_factsnapshottransactionfinancialadjustment`.snapshotdate,
  `ecw_factsnapshottransactionfinancialadjustment`.regionkey,
  `ecw_factsnapshottransactionfinancialadjustment`.coid,
  `ecw_factsnapshottransactionfinancialadjustment`.gldepartment,
  `ecw_factsnapshottransactionfinancialadjustment`.claimkey,
  `ecw_factsnapshottransactionfinancialadjustment`.claimnumber,
  `ecw_factsnapshottransactionfinancialadjustment`.patientkey,
  `ecw_factsnapshottransactionfinancialadjustment`.patientid,
  `ecw_factsnapshottransactionfinancialadjustment`.servicingproviderkey,
  `ecw_factsnapshottransactionfinancialadjustment`.servicingproviderid,
  `ecw_factsnapshottransactionfinancialadjustment`.renderingproviderkey,
  `ecw_factsnapshottransactionfinancialadjustment`.renderingproviderid,
  `ecw_factsnapshottransactionfinancialadjustment`.facilitykey,
  `ecw_factsnapshottransactionfinancialadjustment`.facilityid,
  `ecw_factsnapshottransactionfinancialadjustment`.claimdatekey,
  `ecw_factsnapshottransactionfinancialadjustment`.claimdatemonthid,
  `ecw_factsnapshottransactionfinancialadjustment`.servicedatekey,
  `ecw_factsnapshottransactionfinancialadjustment`.servicedatemonthid,
  `ecw_factsnapshottransactionfinancialadjustment`.iplan1iplankey,
  `ecw_factsnapshottransactionfinancialadjustment`.iplan1id,
  `ecw_factsnapshottransactionfinancialadjustment`.financialclasskey,
  `ecw_factsnapshottransactionfinancialadjustment`.transactionid,
  `ecw_factsnapshottransactionfinancialadjustment`.transactionamt,
  `ecw_factsnapshottransactionfinancialadjustment`.transactiondatekey,
  `ecw_factsnapshottransactionfinancialadjustment`.transactiondatemonthid,
  `ecw_factsnapshottransactionfinancialadjustment`.adjustmentcodekey,
  `ecw_factsnapshottransactionfinancialadjustment`.adjustmentcode,
  `ecw_factsnapshottransactionfinancialadjustment`.dwlastupdatedatetime,
  `ecw_factsnapshottransactionfinancialadjustment`.sourcesystemcode,
  `ecw_factsnapshottransactionfinancialadjustment`.insertedby,
  `ecw_factsnapshottransactionfinancialadjustment`.inserteddtm,
  `ecw_factsnapshottransactionfinancialadjustment`.modifiedby,
  `ecw_factsnapshottransactionfinancialadjustment`.modifieddtm,
  `ecw_factsnapshottransactionfinancialadjustment`.transactiontype,
  `ecw_factsnapshottransactionfinancialadjustment`.adjustmentid,
  `ecw_factsnapshottransactionfinancialadjustment`.transactionsamemonthflag
  FROM
    edwpsc_base_views.`ecw_factsnapshottransactionfinancialadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factsnapshottransactionfinancialadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;