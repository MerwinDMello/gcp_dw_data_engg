CREATE TABLE IF NOT EXISTS edwpsc.cmt_facttreasurybatch
(
  treasurybatchkey INT64 NOT NULL,
  paymentid INT64,
  regionid INT64,
  sourcesystem STRING,
  batchid STRING,
  createdby STRING,
  batchcreatedate DATE,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (treasurybatchkey) NOT ENFORCED
)
;