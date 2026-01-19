CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factencountercmc
(
  encountercmckey INT64 NOT NULL,
  patientmrn STRING,
  visitid STRING,
  patientname STRING,
  servicedatekey DATETIME,
  authoreddatetime DATETIME,
  providername STRING,
  providernpi STRING,
  admitdatetime DATETIME,
  dischargedatetime DATETIME,
  location STRING,
  visitstatus STRING,
  pos STRING,
  derivedfrom STRING,
  sourcereport STRING,
  sourcesystemcode STRING,
  filedate DATETIME,
  filename STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  insertedby INT64,
  inserteddtm DATETIME,
  modifiedby INT64,
  modifieddtm DATETIME,
  docstatus STRING,
  codingstatus STRING
)
;