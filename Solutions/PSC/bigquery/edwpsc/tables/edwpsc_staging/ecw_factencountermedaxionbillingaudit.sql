CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factencountermedaxionbillingaudit
(
  medaxionbillingauditkey INT64 NOT NULL,
  medaxionregionname STRING,
  medaxionlocation STRING,
  servicedatekey DATE,
  servicedatetime DATETIME,
  casenumberfin STRING,
  patientmrn STRING,
  patientname STRING,
  providername STRING,
  providernpi STRING,
  reporttype STRING,
  eventreason STRING,
  firstfiledate DATE,
  lastfiledate DATE,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;