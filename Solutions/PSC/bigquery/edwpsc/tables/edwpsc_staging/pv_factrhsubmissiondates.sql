CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_factrhsubmissiondates
(
  rhsubmissiondatekey INT64 NOT NULL,
  claimkey INT64,
  claimnumber INT64,
  coid STRING,
  regionkey INT64,
  practice STRING,
  claimsubmissiondatekey DATE,
  transmissiontype STRING,
  cpidkey STRING,
  payerindicator STRING,
  payername STRING,
  importdatekey DATE,
  sourceaprimarykeyvalue STRING,
  sourcesystemcode STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (rhsubmissiondatekey) NOT ENFORCED
)
;