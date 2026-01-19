CREATE TABLE IF NOT EXISTS edwpsc.ecw_juncprtxids
(
  ecwprtxidskey INT64 NOT NULL,
  regionkey INT64,
  startsvcdate DATETIME NOT NULL,
  endsvcdate DATETIME,
  providerid INT64,
  providerkey INT64,
  insuranceid INT64,
  iplankey INT64,
  taxid STRING NOT NULL,
  taxidtype STRING,
  billingfacilityid INT64,
  billingfacilitykey INT64,
  siteid STRING,
  apptfacilityid INT64,
  apptfacilitykey INT64,
  allservicedates INT64,
  allappointmentfacilities INT64,
  deleteflag INT64,
  sourceprimarykeyvalue INT64,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;