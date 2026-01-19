CREATE TABLE IF NOT EXISTS edwpsc.ecw_fact53rdpatientpayment
(
  patientpaymentkey INT64 NOT NULL,
  accountnumber STRING,
  patientname STRING,
  ocrscanline STRING,
  statementdate STRING,
  coid STRING,
  practiceid STRING,
  paymentdate STRING,
  paymenttypeflag STRING,
  paymenttype STRING,
  creditcardtypeflag STRING,
  creditcardtype STRING,
  checknumber STRING,
  paidamount NUMERIC(33, 4),
  balancedue NUMERIC(33, 4),
  sourcefilename STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (patientpaymentkey) NOT ENFORCED
)
;