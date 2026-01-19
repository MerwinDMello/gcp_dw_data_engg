CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_facttelstrat
(
  telstratkey INT64 NOT NULL,
  time DATETIME,
  endtime DATETIME,
  useragent STRING,
  ani STRING,
  dnis STRING,
  port STRING,
  portfirstname STRING,
  portlastname STRING,
  direction STRING,
  totalholdtime INT64,
  callduration INT64,
  patientfirstname STRING,
  patientlastname STRING,
  patientid STRING,
  patientkey INT64,
  patientdob STRING,
  customerservicerepid STRING,
  sourcesystemcode STRING,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (telstratkey) NOT ENFORCED
)
;