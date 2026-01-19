CREATE TABLE IF NOT EXISTS edwpsc.ecw_refiplanmeditechecwcrosswalk
(
  meditechecwcrosswalkkey INT64 NOT NULL,
  meditechiplanid STRING,
  ecwiplanname STRING,
  ecwiplanid INT64,
  isgovernmentflag INT64,
  isavailableinecwflag INT64,
  isbillablebymidlevelflag INT64,
  dwlastupdatedatetime DATETIME NOT NULL
)
;