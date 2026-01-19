CREATE OR REPLACE VIEW edwpsc_views.`epic_factsnapshotdiagnosis`
AS SELECT
  `epic_factsnapshotdiagnosis`.diagnosiskey,
  `epic_factsnapshotdiagnosis`.monthid,
  `epic_factsnapshotdiagnosis`.snapshotdate,
  `epic_factsnapshotdiagnosis`.coid,
  `epic_factsnapshotdiagnosis`.regionkey,
  `epic_factsnapshotdiagnosis`.claimkey,
  `epic_factsnapshotdiagnosis`.claimnumber,
  `epic_factsnapshotdiagnosis`.visitnumber,
  `epic_factsnapshotdiagnosis`.gldepartment,
  `epic_factsnapshotdiagnosis`.patientid,
  `epic_factsnapshotdiagnosis`.servicingproviderkey,
  `epic_factsnapshotdiagnosis`.servicingproviderid,
  `epic_factsnapshotdiagnosis`.renderingproviderkey,
  `epic_factsnapshotdiagnosis`.renderingproviderid,
  `epic_factsnapshotdiagnosis`.facilitykey,
  `epic_factsnapshotdiagnosis`.facilityid,
  `epic_factsnapshotdiagnosis`.claimdatekey,
  `epic_factsnapshotdiagnosis`.servicedatekey,
  `epic_factsnapshotdiagnosis`.iplan1iplankey,
  `epic_factsnapshotdiagnosis`.iplan1id,
  `epic_factsnapshotdiagnosis`.financialclasskey,
  `epic_factsnapshotdiagnosis`.diagnosisid,
  `epic_factsnapshotdiagnosis`.diagnosiscode,
  `epic_factsnapshotdiagnosis`.diagnosisorder,
  `epic_factsnapshotdiagnosis`.practicekey,
  `epic_factsnapshotdiagnosis`.practiceid,
  `epic_factsnapshotdiagnosis`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_factsnapshotdiagnosis`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factsnapshotdiagnosis`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;