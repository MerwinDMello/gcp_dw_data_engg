CREATE TABLE IF NOT EXISTS edwpsc_staging.cmt_factmonthlytransactions
(
  tbnumber STRING,
  transactiontype STRING,
  posttype STRING,
  posteddate DATETIME,
  amountposted NUMERIC(31, 2),
  user34id STRING,
  firstname STRING,
  lastname STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;