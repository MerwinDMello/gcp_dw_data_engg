CREATE TABLE IF NOT EXISTS edwpsc.ecw_factsnapshotdiagnosis
(
  diagnosiskey INT64 NOT NULL,
  monthid INT64,
  snapshotdate DATE,
  coid STRING,
  regionkey INT64,
  claimkey INT64,
  claimnumber INT64,
  gldepartment STRING,
  patientid INT64,
  servicingproviderkey INT64,
  servicingproviderid INT64,
  renderingproviderkey INT64,
  renderingproviderid INT64,
  facilitykey INT64,
  facilityid INT64,
  claimdatekey DATE,
  servicedatekey DATE,
  iplan1iplankey INT64,
  iplan1id INT64,
  financialclasskey INT64,
  diagnosisid INT64,
  diagnosiscode STRING,
  diagnosisorder INT64,
  dwlastupdatedatetime DATETIME
)
PARTITION BY snapshotdate
;