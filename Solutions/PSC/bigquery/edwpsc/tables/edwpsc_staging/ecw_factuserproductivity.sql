CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factuserproductivity
(
  loadkey INT64 NOT NULL,
  loaddatekey DATE NOT NULL,
  userproductivitykey INT64,
  bsuid INT64,
  processid INT64,
  controllnumber INT64,
  employeegroupname STRING,
  actiondate DATE,
  actionuserid STRING,
  claimkey INT64,
  invoicenumber STRING,
  coid STRING,
  regionkey INT64,
  guarantorid INT64,
  accountnumber STRING,
  prpnumber INT64,
  ppikey STRING,
  documenthandle INT64,
  batchid STRING,
  sourcesystem STRING,
  pool STRING,
  poolfunction STRING,
  dwlastupdatedatetime DATE NOT NULL
)
;