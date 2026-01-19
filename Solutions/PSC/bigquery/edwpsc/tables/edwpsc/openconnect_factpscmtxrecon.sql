CREATE TABLE IF NOT EXISTS edwpsc.openconnect_factpscmtxrecon
(
  openconnectpscmtxreconkey INT64 NOT NULL,
  messagecontrolid STRING,
  batchdate DATE,
  visitnumber STRING,
  patientaccountnumber STRING,
  procedurecode STRING,
  cptcode STRING,
  servicedate DATE,
  cptunits FLOAT64,
  facilitylocationid STRING,
  coid STRING,
  filedate DATETIME,
  filename STRING,
  fileimporteddate DATETIME,
  sourcefiletype STRING,
  sendingmtx STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;