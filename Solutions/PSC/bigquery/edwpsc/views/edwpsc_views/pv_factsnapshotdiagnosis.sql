CREATE OR REPLACE VIEW edwpsc_views.`pv_factsnapshotdiagnosis`
AS SELECT
  `pv_factsnapshotdiagnosis`.diagnosiskey,
  `pv_factsnapshotdiagnosis`.monthid,
  `pv_factsnapshotdiagnosis`.snapshotdate,
  `pv_factsnapshotdiagnosis`.coid,
  `pv_factsnapshotdiagnosis`.regionkey,
  `pv_factsnapshotdiagnosis`.claimkey,
  `pv_factsnapshotdiagnosis`.claimnumber,
  `pv_factsnapshotdiagnosis`.gldepartment,
  `pv_factsnapshotdiagnosis`.patientid,
  `pv_factsnapshotdiagnosis`.servicingproviderkey,
  `pv_factsnapshotdiagnosis`.servicingproviderid,
  `pv_factsnapshotdiagnosis`.renderingproviderkey,
  `pv_factsnapshotdiagnosis`.renderingproviderid,
  `pv_factsnapshotdiagnosis`.facilitykey,
  `pv_factsnapshotdiagnosis`.facilityid,
  `pv_factsnapshotdiagnosis`.claimdatekey,
  `pv_factsnapshotdiagnosis`.servicedatekey,
  `pv_factsnapshotdiagnosis`.iplan1iplankey,
  `pv_factsnapshotdiagnosis`.iplan1id,
  `pv_factsnapshotdiagnosis`.financialclasskey,
  `pv_factsnapshotdiagnosis`.diagnosisid,
  `pv_factsnapshotdiagnosis`.diagnosiscode,
  `pv_factsnapshotdiagnosis`.diagnosisorder,
  `pv_factsnapshotdiagnosis`.practicekey,
  `pv_factsnapshotdiagnosis`.practiceid,
  `pv_factsnapshotdiagnosis`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_factsnapshotdiagnosis`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factsnapshotdiagnosis`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;