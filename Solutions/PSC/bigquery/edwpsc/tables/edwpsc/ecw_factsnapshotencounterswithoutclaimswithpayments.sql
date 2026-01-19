CREATE TABLE IF NOT EXISTS edwpsc.ecw_factsnapshotencounterswithoutclaimswithpayments
(
  encounterswithoutclaimswithpaymentskey INT64 NOT NULL,
  monthid INT64,
  snapshotdate DATE,
  coid STRING,
  finacialclasskey INT64,
  facilityid INT64,
  facilitykey INT64 NOT NULL,
  encounterid INT64,
  encounterkey INT64 NOT NULL,
  dosproviderid INT64,
  servicingproviderkey INT64 NOT NULL,
  totalpaymentamt NUMERIC(33, 4),
  totalpatientpaymentamt NUMERIC(33, 4),
  lastpaymentdatekey DATETIME,
  lastpatientpaymentdatekey DATETIME,
  servicedate DATETIME,
  patientid INT64,
  patientkey INT64 NOT NULL,
  totalbalanceamt NUMERIC(33, 4),
  practiceid INT64,
  practicekey INT64 NOT NULL,
  regionkey INT64
)
PARTITION BY snapshotdate
;