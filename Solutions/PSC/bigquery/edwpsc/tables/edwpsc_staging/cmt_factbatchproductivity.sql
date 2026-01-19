CREATE TABLE IF NOT EXISTS edwpsc_staging.cmt_factbatchproductivity
(
  batchproductivitykey INT64 NOT NULL,
  userid STRING,
  firstname STRING,
  lastname STRING,
  batchid STRING NOT NULL,
  taxid STRING,
  bankpayername STRING,
  dateofaction DATE,
  timeofaction TIME,
  batchstatusbeforeaction STRING,
  batchstatusafteraction STRING,
  action STRING,
  notes STRING,
  coid STRING,
  amount NUMERIC(31, 2),
  checknumber STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;