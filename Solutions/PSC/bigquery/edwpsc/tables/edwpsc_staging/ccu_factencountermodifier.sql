CREATE TABLE IF NOT EXISTS edwpsc_staging.ccu_factencountermodifier
(
  id INT64,
  encounterid INT64,
  invoiceid INT64,
  patientid INT64,
  displayindex INT64,
  itemid INT64,
  meddesc STRING,
  date DATETIME,
  deleteflag INT64,
  encmod1 STRING,
  encmod2 STRING,
  encmod3 STRING,
  encmod4 STRING,
  created DATETIME,
  modified DATETIME,
  dwlastupdatedatetime DATETIME,
  regionkey INT64,
  ccuencountermodifierkey INT64 NOT NULL
)
;