CREATE TABLE IF NOT EXISTS edwpsc.epic_factbilldeniedcharge
(
  sourcepaymenttransactionid STRING,
  billdeniedchargereceiveddate DATE,
  billdeniedchargeamount NUMERIC(33, 4),
  billdeniedchargeid INT64,
  billdeniedchargename STRING,
  invoicenumber STRING,
  denialcode STRING,
  denialcodedesc STRING,
  denialcoderanking INT64,
  regionkey INT64,
  sourceaprimarykeyvalue INT64,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  billdeniedchargekey INT64 NOT NULL
)
;