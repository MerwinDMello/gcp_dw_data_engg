CREATE TABLE IF NOT EXISTS edwpsc.ecw_rprticcchargelag
(
  iccchargelagkey INT64 NOT NULL,
  firsttransactionmonth DATE,
  regionkey INT64,
  groupname STRING,
  divisionname STRING,
  marketname STRING,
  lobname STRING,
  coid STRING,
  coidname STRING,
  servicingprovidername STRING,
  renderingprovidername STRING,
  claimkey INT64,
  claimnumber INT64,
  lastclaimnumber INT64,
  voidflag INT64,
  voidandrecreatedate DATE,
  servicedatekey DATE,
  lastvoiddatekey DATE,
  firsttransactiondatekey DATE,
  lag INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  snapshotdate DATE,
  PRIMARY KEY (iccchargelagkey) NOT ENFORCED
)
PARTITION BY snapshotdate
;