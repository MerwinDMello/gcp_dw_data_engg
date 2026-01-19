CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factmeditechvisitempi
(
  meditechvisitempikey INT64 NOT NULL,
  coid STRING,
  meditechcocid STRING,
  empinumber INT64,
  accountnumber STRING,
  patientaccountnumber INT64,
  patientmedicalrecord STRING,
  patientdwid INT64,
  sourceaprimarykeyvalue STRING,
  sourcelastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;