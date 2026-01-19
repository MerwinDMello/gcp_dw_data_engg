CREATE TABLE IF NOT EXISTS edwpsc.epic_factsnapshotencounterswithoutclaimswithpayments
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
  dosproviderid STRING,
  servicingproviderkey INT64 NOT NULL,
  totalpaymentamt NUMERIC(33, 4),
  totalpatientpaymentamt NUMERIC(33, 4),
  lastpaymentdatekey DATETIME,
  lastpatientpaymentdatekey DATETIME,
  servicedate DATETIME,
  patientid INT64,
  patientkey INT64 NOT NULL,
  totalbalanceamt NUMERIC(33, 4),
  practicekey INT64 NOT NULL,
  regionkey INT64
)
PARTITION BY snapshotdate
;