CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_experityclaimdata
(
  id INT64 NOT NULL,
  filename STRING,
  claimnumber STRING,
  claimamount NUMERIC(31, 2),
  batchid STRING,
  claimsource STRING,
  appenddate DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (id) NOT ENFORCED
)
;