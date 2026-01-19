CREATE TABLE IF NOT EXISTS edwpsc.openconnect_reffacilitycrosswalk
(
  openconnectfacilitycrosswalkkey INT64 NOT NULL,
  regionkey INT64,
  sendingapplication STRING,
  facilityid STRING,
  practice STRING,
  visitlocationunit STRING,
  patientclass STRING,
  ecwhl7id STRING,
  beginactive DATE,
  endactive DATE,
  crosswalkcreatedby STRING,
  crosswalkcreateddate DATETIME,
  crosswalkmodifiedby STRING,
  crosswalkmodifieddate DATETIME,
  deletedflag INT64,
  ruralhealthflag INT64,
  version INT64,
  sourceaprimarykeyvalue INT64 NOT NULL,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  coid STRING
)
;