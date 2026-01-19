CREATE OR REPLACE VIEW edwpsc_views.`pv_factsnapshotfinancialadjustmentclaim`
AS SELECT
  `pv_factsnapshotfinancialadjustmentclaim`.adjustmentclaimkey,
  `pv_factsnapshotfinancialadjustmentclaim`.monthid,
  `pv_factsnapshotfinancialadjustmentclaim`.snapshotdate,
  `pv_factsnapshotfinancialadjustmentclaim`.coid,
  `pv_factsnapshotfinancialadjustmentclaim`.regionkey,
  `pv_factsnapshotfinancialadjustmentclaim`.claimkey,
  `pv_factsnapshotfinancialadjustmentclaim`.claimnumber,
  `pv_factsnapshotfinancialadjustmentclaim`.gldepartment,
  `pv_factsnapshotfinancialadjustmentclaim`.patientid,
  `pv_factsnapshotfinancialadjustmentclaim`.servicingproviderkey,
  `pv_factsnapshotfinancialadjustmentclaim`.servicingproviderid,
  `pv_factsnapshotfinancialadjustmentclaim`.renderingproviderkey,
  `pv_factsnapshotfinancialadjustmentclaim`.renderingproviderid,
  `pv_factsnapshotfinancialadjustmentclaim`.facilitykey,
  `pv_factsnapshotfinancialadjustmentclaim`.facilityid,
  `pv_factsnapshotfinancialadjustmentclaim`.claimdatekey,
  `pv_factsnapshotfinancialadjustmentclaim`.servicedatekey,
  `pv_factsnapshotfinancialadjustmentclaim`.iplan1iplankey,
  `pv_factsnapshotfinancialadjustmentclaim`.iplan1id,
  `pv_factsnapshotfinancialadjustmentclaim`.financialclasskey,
  `pv_factsnapshotfinancialadjustmentclaim`.adjustmentid,
  `pv_factsnapshotfinancialadjustmentclaim`.adjustmentcode,
  `pv_factsnapshotfinancialadjustmentclaim`.adjustmentcodekey,
  `pv_factsnapshotfinancialadjustmentclaim`.adjustmentamt,
  `pv_factsnapshotfinancialadjustmentclaim`.unpostedcptamt,
  `pv_factsnapshotfinancialadjustmentclaim`.unpostedclaimamt,
  `pv_factsnapshotfinancialadjustmentclaim`.adjustmentcreatedatekey,
  `pv_factsnapshotfinancialadjustmentclaim`.adjustmentmodifieddatekey,
  `pv_factsnapshotfinancialadjustmentclaim`.practicekey,
  `pv_factsnapshotfinancialadjustmentclaim`.practiceid,
  `pv_factsnapshotfinancialadjustmentclaim`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_factsnapshotfinancialadjustmentclaim`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factsnapshotfinancialadjustmentclaim`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;