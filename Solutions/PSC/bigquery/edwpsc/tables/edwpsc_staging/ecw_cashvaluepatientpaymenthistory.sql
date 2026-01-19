CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_cashvaluepatientpaymenthistory
(
  patientbilltype STRING,
  coid STRING,
  deptcode STRING,
  marketkey INT64,
  specialtycategorykey INT64,
  renderingproviderkey INT64,
  guarantorpatientkey INT64,
  claimkey INT64,
  firstpatientstatementbilldatekey DATE,
  lastpatientstatementbilldatekey DATE,
  numberofpatientstatements INT64,
  lastpatientclaimpaymentdatekey DATE,
  bankdaystopayment INT64,
  totalpatientresponsibilityamt NUMERIC(33, 4),
  balancebilledtopatient NUMERIC(33, 4),
  upfrontpaymentamt NUMERIC(33, 4),
  postbillpaymentamt NUMERIC(33, 4),
  totalpatientpaymentsamt NUMERIC(33, 4),
  totalpatientbalanceamt NUMERIC(33, 4),
  dwlastupdatedatetime DATETIME
)
;