CREATE TABLE IF NOT EXISTS edwpsc_staging.cmt_factbatchexceptionsdetail
(
  batchid STRING,
  batchdate DATETIME,
  depositdate STRING,
  locationname STRING,
  locationcode STRING,
  notes STRING,
  transactiontypeid INT64,
  transactiontype STRING,
  depositamount NUMERIC(31, 2),
  postamount BIGNUMERIC(40, 2),
  variance BIGNUMERIC(40, 2),
  coid STRING,
  specialist STRING,
  statusnotes STRING,
  age INT64,
  batchage INT64,
  batchreasonid STRING,
  workflowstatus STRING,
  specialistid INT64,
  batchexceptionsdetailkey INT64 NOT NULL,
  dwlastupdatedatetime DATETIME,
  selectedcoiduser STRING
)
;