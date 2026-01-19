CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeeconbase
(
  pspeecobdate DATETIME,
  pspeecobdesc STRING,
  pspeecobectid STRING,
  pspeecobexportdte DATETIME,
  pspeecobhandle STRING,
  pspeecobid NUMERIC(29) NOT NULL,
  pspeecobnew STRING,
  pspeecobportalectid STRING,
  pspeecobportalid STRING,
  pspeecobportalkey STRING,
  pspeecobppiid STRING,
  pspeecobsource STRING,
  pspeecobtime TIME,
  pspeecobuser STRING,
  PRIMARY KEY (pspeecobid) NOT ENFORCED
)
;