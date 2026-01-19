CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_reffinancialclassepic
(
  financialclasskey INT64 NOT NULL,
  financialclasscode STRING,
  financialclassname STRING,
  financialclassabbr STRING,
  finclassc STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  financialclassispatientbalance INT64 NOT NULL,
  PRIMARY KEY (financialclasskey) NOT ENFORCED
)
;