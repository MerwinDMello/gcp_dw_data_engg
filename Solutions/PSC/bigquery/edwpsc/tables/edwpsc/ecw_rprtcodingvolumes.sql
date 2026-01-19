CREATE TABLE IF NOT EXISTS edwpsc.ecw_rprtcodingvolumes
(
  codingvolumekey INT64 NOT NULL,
  coid STRING,
  claimkey INT64,
  claimnumber INT64,
  regionkey INT64,
  patientkey INT64,
  servicingproviderkey INT64,
  claimlinechargekey INT64,
  transactionbyuserkey INT64,
  transactiondatekey DATE,
  systemname STRING,
  praticename STRING,
  visitnumber INT64,
  rownumber INT64,
  recordtype STRING,
  snapshotdate DATE,
  deptcode STRING,
  usertype STRING,
  placeofservice INT64,
  claimstatusname STRING,
  visittypename STRING
)
PARTITION BY snapshotdate
;