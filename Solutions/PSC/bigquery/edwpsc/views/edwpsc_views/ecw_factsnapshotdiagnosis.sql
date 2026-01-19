CREATE OR REPLACE VIEW edwpsc_views.`ecw_factsnapshotdiagnosis`
AS SELECT
  `ecw_factsnapshotdiagnosis`.diagnosiskey,
  `ecw_factsnapshotdiagnosis`.monthid,
  `ecw_factsnapshotdiagnosis`.snapshotdate,
  `ecw_factsnapshotdiagnosis`.coid,
  `ecw_factsnapshotdiagnosis`.regionkey,
  `ecw_factsnapshotdiagnosis`.claimkey,
  `ecw_factsnapshotdiagnosis`.claimnumber,
  `ecw_factsnapshotdiagnosis`.gldepartment,
  `ecw_factsnapshotdiagnosis`.patientid,
  `ecw_factsnapshotdiagnosis`.servicingproviderkey,
  `ecw_factsnapshotdiagnosis`.servicingproviderid,
  `ecw_factsnapshotdiagnosis`.renderingproviderkey,
  `ecw_factsnapshotdiagnosis`.renderingproviderid,
  `ecw_factsnapshotdiagnosis`.facilitykey,
  `ecw_factsnapshotdiagnosis`.facilityid,
  `ecw_factsnapshotdiagnosis`.claimdatekey,
  `ecw_factsnapshotdiagnosis`.servicedatekey,
  `ecw_factsnapshotdiagnosis`.iplan1iplankey,
  `ecw_factsnapshotdiagnosis`.iplan1id,
  `ecw_factsnapshotdiagnosis`.financialclasskey,
  `ecw_factsnapshotdiagnosis`.diagnosisid,
  `ecw_factsnapshotdiagnosis`.diagnosiscode,
  `ecw_factsnapshotdiagnosis`.diagnosisorder,
  `ecw_factsnapshotdiagnosis`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_factsnapshotdiagnosis`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factsnapshotdiagnosis`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;