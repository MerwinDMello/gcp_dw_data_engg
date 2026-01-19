CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refiplanmeditechpacrosswalk
(
  meditechpacrosswalkkey INT64 NOT NULL,
  sscstate STRING,
  sscname STRING,
  hospitalcoid STRING,
  meditechiplanid STRING,
  paiplanid INT64,
  paiplanname STRING,
  paiplanfinancialclasskey INT64,
  painsuranceactiveflag STRING,
  dwlastupdatedatetime DATETIME NOT NULL
)
;