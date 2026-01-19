CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refcptworkrvu
(
  cptrvukey INT64 NOT NULL,
  cptcode STRING NOT NULL,
  year INT64 NOT NULL,
  cptmodifier STRING NOT NULL,
  cptdescription STRING,
  workrvustat NUMERIC(33, 4) NOT NULL,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (cptcode, year, cptmodifier) NOT ENFORCED
)
;