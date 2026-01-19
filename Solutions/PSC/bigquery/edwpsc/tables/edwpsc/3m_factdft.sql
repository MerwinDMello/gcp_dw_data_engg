CREATE TABLE IF NOT EXISTS edwpsc.`3m_factdft`
(
  dft3mkey INT64 NOT NULL,
  batchdatekey DATE,
  batchdatetime TIME,
  visitnumber STRING,
  patientaccountnumber STRING,
  procedurecode STRING,
  proceduretype STRING,
  servicedate DATE,
  cptunit NUMERIC(31, 2),
  facilityid STRING,
  sendernote STRING,
  filename STRING,
  sourceaprimarykeyvalue STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;