CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspepreducation
(
  pspepredcertnum STRING,
  pspepreddegree STRING,
  pspepredenddte DATETIME,
  pspepredgafid STRING,
  pspepredinid STRING,
  pspepredkey STRING NOT NULL,
  pspepredperfid STRING,
  pspepredprogram STRING,
  pspepredspecialty STRING,
  pspepredstdte DATETIME,
  pspepredtitle STRING,
  PRIMARY KEY (pspepredkey) NOT ENFORCED
)
;