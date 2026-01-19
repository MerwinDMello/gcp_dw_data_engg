CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factencountermedaxionworkqueue
(
  dateofservice DATE,
  dayssincedateofservice INT64,
  currentqueueentrydate DATE,
  daysincurrentqueue INT64,
  casestatus STRING,
  casenumberfin STRING,
  patientnumbermrn STRING,
  patientfirst3 STRING,
  patientlast3 STRING,
  patientdob DATE,
  providername STRING,
  supervisorname STRING,
  payer STRING,
  location STRING,
  workqueuecategory STRING,
  filename STRING,
  filedate DATE,
  coid STRING,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;