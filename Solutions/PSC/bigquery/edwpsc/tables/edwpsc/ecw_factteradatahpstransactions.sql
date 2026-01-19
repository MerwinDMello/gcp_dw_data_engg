CREATE TABLE IF NOT EXISTS edwpsc.ecw_factteradatahpstransactions
(
  teradatahpstransactionskey INT64 NOT NULL,
  coid STRING,
  regionkey INT64,
  loaddatekey DATE,
  hpstransactionid NUMERIC(29),
  transactiontimestamp TIME,
  transactiondate DATE,
  entryuserid STRING,
  paymenttendertype STRING,
  paymentreference STRING,
  transactionamt NUMERIC(33, 4),
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (teradatahpstransactionskey) NOT ENFORCED
)
;