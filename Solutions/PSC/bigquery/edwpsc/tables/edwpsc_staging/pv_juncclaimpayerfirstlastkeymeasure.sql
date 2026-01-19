CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_juncclaimpayerfirstlastkeymeasure
(
  claimpayerkey INT64 NOT NULL,
  claimkey INT64 NOT NULL,
  coid STRING,
  payerfirstbillkey INT64,
  payerfirstbilldatekey DATE,
  payerlastbillkey INT64,
  payerlastbilldatekey DATE,
  payernumberofbills INT64,
  payerfirstclaimpaymentkey INT64,
  payerfirstclaimpaymentdatekey DATE,
  payerlastclaimpaymentkey INT64,
  payerlastclaimpaymentdatekey DATE,
  payertotalpayments NUMERIC(33, 4),
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (claimpayerkey) NOT ENFORCED
)
;