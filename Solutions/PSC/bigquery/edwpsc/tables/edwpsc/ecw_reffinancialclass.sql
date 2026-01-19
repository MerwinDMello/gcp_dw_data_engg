CREATE TABLE IF NOT EXISTS edwpsc.ecw_reffinancialclass
(
  financialclasskey INT64 NOT NULL,
  financialclassname STRING NOT NULL,
  financialclassispatientbalance INT64 NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (financialclasskey) NOT ENFORCED
)
;