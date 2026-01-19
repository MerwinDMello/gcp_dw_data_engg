CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refchargevalue
(
  chargevaluekey INT64 NOT NULL,
  chargevaluename STRING NOT NULL,
  chargevaluetype STRING NOT NULL,
  chargevaluedesc STRING NOT NULL,
  chargevalueprioritynum INT64 NOT NULL,
  chargevaluefrequency STRING NOT NULL,
  chargevaluequery STRING NOT NULL,
  chargevalueconfidencelevelpercent NUMERIC(33, 4) NOT NULL,
  chargevalueconfidencelastvalidateddate DATE NOT NULL,
  chargevaluecreatedby STRING NOT NULL,
  chargevaluecreateddatekey DATE NOT NULL,
  chargevaluelastmodifiedby STRING,
  chargevaluelastprocesseddate DATETIME,
  chargevaluelasterrormessage STRING,
  enabled INT64,
  sysstarttime DATETIME NOT NULL,
  sysendtime DATETIME NOT NULL,
  developeremail STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  PRIMARY KEY (chargevaluekey) NOT ENFORCED
)
;