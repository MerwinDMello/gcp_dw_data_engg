CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_juncpaymentmatchingtransactions
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