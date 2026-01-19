CREATE TABLE IF NOT EXISTS edwpsc.epic_refpbbillingstat
(
  pbbillingstatkey INT64 NOT NULL,
  pbbillingstatname STRING,
  pbbillingstattitle STRING,
  pbbillingstatabbr STRING,
  pbbillingstatinternalid STRING,
  pbbillingstatusc STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (pbbillingstatkey) NOT ENFORCED
)
;