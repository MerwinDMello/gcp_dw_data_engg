CREATE TABLE IF NOT EXISTS edwpsc.epic_factsnapshotdiagnosis
(
  diagnosiskey INT64 NOT NULL,
  monthid INT64,
  snapshotdate DATE,
  coid STRING,
  regionkey INT64,
  claimkey INT64,
  claimnumber INT64,
  visitnumber INT64,
  gldepartment STRING,
  patientid INT64,
  servicingproviderkey INT64,
  servicingproviderid STRING,
  renderingproviderkey INT64,
  renderingproviderid STRING,
  facilitykey INT64,
  facilityid INT64,
  claimdatekey DATE,
  servicedatekey DATE,
  iplan1iplankey INT64,
  iplan1id STRING,
  financialclasskey INT64,
  diagnosisid INT64,
  diagnosiscode STRING,
  diagnosisorder INT64,
  practicekey INT64,
  practiceid STRING,
  dwlastupdatedatetime DATETIME
)
PARTITION BY snapshotdate
;