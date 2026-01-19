CREATE TABLE IF NOT EXISTS edwpsc.epic_juncpaymentmatchingtransactions
(
  transactionid INT64 NOT NULL,
  matchingtransactionid INT64,
  regionkey INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;