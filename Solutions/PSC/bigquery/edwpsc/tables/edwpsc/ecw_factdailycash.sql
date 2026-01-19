CREATE TABLE IF NOT EXISTS edwpsc.ecw_factdailycash
(
  dailycashkey INT64 NOT NULL,
  dateposted DATE,
  sourcesystem STRING,
  icccoidflag STRING,
  paymenttype STRING,
  amount NUMERIC(33, 4),
  pcnr2moprior NUMERIC(33, 4),
  totalbankdays INT64,
  runningtotalbankdays INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (dailycashkey) NOT ENFORCED
)
;