CREATE TABLE IF NOT EXISTS edwpsc.epic_refpaymentsource
(
  paymentsourcekey INT64 NOT NULL,
  paymentsourcename STRING,
  paymentsourcetitle STRING,
  paymentsourceabbr STRING,
  paymentsourceinternalid STRING,
  paymentsourcec STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (paymentsourcekey) NOT ENFORCED
)
;