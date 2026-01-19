DROP TABLE IF EXISTS edwpsc_staging.ecw_juncclaimpayerfirstlastkeymeasure;
DROP TABLE IF EXISTS edwpsc.ecw_juncclaimpayerfirstlastkeymeasure;

CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_juncclaimpayerfirstlastkeymeasure
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
  archivedrecord STRING NOT NULL,
  PRIMARY KEY (claimpayerkey, claimkey) NOT ENFORCED
)
;

CREATE TABLE IF NOT EXISTS edwpsc.ecw_juncclaimpayerfirstlastkeymeasure
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
  archivedrecord STRING NOT NULL,
  PRIMARY KEY (claimpayerkey, claimkey) NOT ENFORCED
)
;

TRUNCATE TABLE edwpsc_staging.ecw_juncclaimpayerfirstlastkeymeasure;
INSERT INTO edwpsc_staging.ecw_juncclaimpayerfirstlastkeymeasure
SELECT * FROM prod_support.ecw_juncclaimpayerfirstlastkeymeasure_stg_20251219;


TRUNCATE TABLE edwpsc.ecw_juncclaimpayerfirstlastkeymeasure;
INSERT INTO edwpsc.ecw_juncclaimpayerfirstlastkeymeasure
SELECT * FROM prod_support.ecw_juncclaimpayerfirstlastkeymeasure_20251219;