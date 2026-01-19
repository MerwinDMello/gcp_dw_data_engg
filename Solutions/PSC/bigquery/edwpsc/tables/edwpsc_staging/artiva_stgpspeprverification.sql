CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeprverification
(
  pspeprvafid STRING,
  pspeprvaviid STRING,
  pspeprvbarcode STRING,
  pspeprvbdid STRING,
  pspeprvedid STRING,
  pspeprvgafid STRING,
  pspeprvhowv STRING,
  pspeprvinsid STRING,
  pspeprvlicid STRING,
  pspeprvperfid STRING,
  pspeprvverified STRING,
  pspeprvverifydte DATETIME,
  pspeprvverifyuser STRING,
  pspeprvkey STRING NOT NULL,
  PRIMARY KEY (pspeprvkey) NOT ENFORCED
)
;