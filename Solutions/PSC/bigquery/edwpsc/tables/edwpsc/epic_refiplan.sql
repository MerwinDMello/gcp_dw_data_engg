CREATE TABLE IF NOT EXISTS edwpsc.epic_refiplan
(
  iplankey INT64 NOT NULL,
  iplanname STRING,
  iplanshortname STRING,
  iplanaddress1 STRING,
  iplanaddress2 STRING,
  iplancity STRING,
  iplanstate STRING,
  iplanzip STRING,
  iplanphone STRING,
  payorid STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  iplanprimarygeographykey INT64,
  iplangroupfinancialkey INT64,
  financialclasskey INT64,
  epicfinancialclass INT64,
  epicfinancialclassdesc STRING,
  epicfinancialclassgrouped STRING,
  PRIMARY KEY (iplankey) NOT ENFORCED
)
;