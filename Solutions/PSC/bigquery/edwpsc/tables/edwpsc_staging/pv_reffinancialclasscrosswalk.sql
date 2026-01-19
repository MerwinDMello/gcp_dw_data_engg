CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_reffinancialclasscrosswalk
(
  financialclasskey INT64 NOT NULL,
  sourcefinancialclass STRING NOT NULL,
  ispatientflag INT64,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  PRIMARY KEY (financialclasskey, sourcefinancialclass) NOT ENFORCED
)
;