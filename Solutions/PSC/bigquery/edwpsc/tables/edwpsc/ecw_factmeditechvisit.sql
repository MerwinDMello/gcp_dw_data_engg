CREATE TABLE IF NOT EXISTS edwpsc.ecw_factmeditechvisit
(
  meditechvisitkey INT64 NOT NULL,
  sendingapplication STRING,
  uniquechargeidentifier STRING,
  facilityid STRING,
  practiceid STRING,
  mrn STRING,
  visitnumber STRING,
  admitdate DATETIME,
  dischargedate DATETIME,
  patientname STRING,
  billingprovidername STRING,
  coid STRING,
  sourcesystemcode STRING,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  patientage INT64,
  dealicensenumber STRING,
  providerusername STRING,
  npi STRING,
  visitlocationunit STRING,
  censuscoid STRING
)
;