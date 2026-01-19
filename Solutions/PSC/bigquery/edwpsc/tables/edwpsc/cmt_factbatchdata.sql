CREATE TABLE IF NOT EXISTS edwpsc.cmt_factbatchdata
(
  batchdatakey INT64 NOT NULL,
  batchid STRING NOT NULL,
  userid STRING,
  updatedate DATETIME,
  historyreasonid INT64,
  historyreasoncode STRING,
  batchstate INT64,
  batchstatename STRING,
  sourceaprimarykeyvalue INT64 NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  payername STRING,
  checknumber STRING,
  dwlastupdatedatetime DATETIME
)
;