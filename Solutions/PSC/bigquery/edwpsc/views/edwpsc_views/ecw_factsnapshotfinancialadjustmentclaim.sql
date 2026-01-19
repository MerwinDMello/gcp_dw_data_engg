CREATE OR REPLACE VIEW edwpsc_views.`ecw_factsnapshotfinancialadjustmentclaim`
AS SELECT
  `ecw_factsnapshotfinancialadjustmentclaim`.adjustmentclaimkey,
  `ecw_factsnapshotfinancialadjustmentclaim`.monthid,
  `ecw_factsnapshotfinancialadjustmentclaim`.snapshotdate,
  `ecw_factsnapshotfinancialadjustmentclaim`.coid,
  `ecw_factsnapshotfinancialadjustmentclaim`.regionkey,
  `ecw_factsnapshotfinancialadjustmentclaim`.claimkey,
  `ecw_factsnapshotfinancialadjustmentclaim`.claimnumber,
  `ecw_factsnapshotfinancialadjustmentclaim`.gldepartment,
  `ecw_factsnapshotfinancialadjustmentclaim`.patientid,
  `ecw_factsnapshotfinancialadjustmentclaim`.servicingproviderkey,
  `ecw_factsnapshotfinancialadjustmentclaim`.servicingproviderid,
  `ecw_factsnapshotfinancialadjustmentclaim`.renderingproviderkey,
  `ecw_factsnapshotfinancialadjustmentclaim`.renderingproviderid,
  `ecw_factsnapshotfinancialadjustmentclaim`.facilitykey,
  `ecw_factsnapshotfinancialadjustmentclaim`.facilityid,
  `ecw_factsnapshotfinancialadjustmentclaim`.claimdatekey,
  `ecw_factsnapshotfinancialadjustmentclaim`.servicedatekey,
  `ecw_factsnapshotfinancialadjustmentclaim`.iplan1iplankey,
  `ecw_factsnapshotfinancialadjustmentclaim`.iplan1id,
  `ecw_factsnapshotfinancialadjustmentclaim`.financialclasskey,
  `ecw_factsnapshotfinancialadjustmentclaim`.adjustmentid,
  `ecw_factsnapshotfinancialadjustmentclaim`.adjustmentcode,
  `ecw_factsnapshotfinancialadjustmentclaim`.adjustmentcodekey,
  `ecw_factsnapshotfinancialadjustmentclaim`.adjustmentamt,
  `ecw_factsnapshotfinancialadjustmentclaim`.unpostedcptamt,
  `ecw_factsnapshotfinancialadjustmentclaim`.unpostedclaimamt,
  `ecw_factsnapshotfinancialadjustmentclaim`.adjustmentcreatedatekey,
  `ecw_factsnapshotfinancialadjustmentclaim`.adjustmentmodifieddatekey,
  `ecw_factsnapshotfinancialadjustmentclaim`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_factsnapshotfinancialadjustmentclaim`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factsnapshotfinancialadjustmentclaim`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;