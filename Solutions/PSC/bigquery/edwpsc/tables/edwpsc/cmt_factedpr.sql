CREATE TABLE IF NOT EXISTS edwpsc.cmt_factedpr
(
  reportid INT64,
  reportname STRING,
  coid STRING,
  servicedate DATE,
  practicename STRING,
  location STRING,
  reportstatus STRING,
  reportstatusid INT64,
  bank INT64,
  enddate DATE,
  hasdeposit BOOL,
  deleteisallowed BOOL,
  dwlastupdatedatetime DATETIME,
  batchexceptionsdetailkey INT64 NOT NULL,
  lastsaved DATETIME,
  batchnbr STRING,
  createddate DATETIME
)
;